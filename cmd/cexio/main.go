package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/nikandfor/cexio"
	"github.com/nikandfor/cli"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/tlog"
	"github.com/nikandfor/tlog/ext/tlflag"
	"github.com/shopspring/decimal"
)

func main() {
	cli.App = cli.Command{
		Name:   "cex cli",
		Before: before,
		Flags: []*cli.Flag{
			cli.NewFlag("key", "", "api key"),
			cli.NewFlag("secret", "", "api key secret"),

			cli.NewFlag("pair", "BTC:USD", "pair"),

			cli.NewFlag("log", "stderr+dm", "log destination"),
			cli.NewFlag("v", "", "verbosity topics"),
			cli.NewFlag("debug", "", "debug addr to listen to", cli.Hidden),

			cli.FlagfileFlag,
			cli.HelpFlag,
		},
		Commands: []*cli.Command{{
			Name:   "public",
			Action: subscribe,
			Args:   cli.Args{},
		}, {
			Name:   "tick,ticker",
			Action: ticker,
		}, {
			Name:   "balance",
			Action: balance,
		}, {
			Name:   "orderbook",
			Action: orderbook,
		}, {
			Name:   "order",
			Action: orders,
			Commands: []*cli.Command{{
				Name:   "place,new,add",
				Action: orderPlace,
				Flags: []*cli.Flag{
					cli.NewFlag("action", "", "buy|sell"),
					cli.NewFlag("price", "", ""),
					cli.NewFlag("amount", "", ""),
				},
			}, {
				Name:   "cancel",
				Action: orderCancel,
				Args:   cli.Args{},
			}},
		}},
	}

	cli.RunAndExit(os.Args)
}

func before(c *cli.Command) error {
	w, err := tlflag.OpenWriter(c.String("log"))
	if err != nil {
		return errors.Wrap(err, "parse log flag")
	}

	tlog.DefaultLogger = tlog.New(w)

	tlog.SetFilter(c.String("v"))

	ls := tlog.FillLabelsWithDefaults("service=cexio", "_hostname", "_runid", "_execmd5")

	tlog.SetLabels(ls)

	if a := c.String("debug"); a != "" {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)

		go func() {
			err := http.ListenAndServe(a, nil)
			tlog.Printw("debug server", "err", err)
			os.Exit(1)
		}()

		tlog.Printf("listen debug server on %v", a)
	}

	return nil
}

func subscribe(c *cli.Command) (err error) {
	cl, err := cexio.New(c.String("key"), []byte(c.String("secret")))
	if err != nil {
		return errors.Wrap(err, "new client")
	}

	ws, err := cl.Websocket(context.Background())
	if err != nil {
		return errors.Wrap(err, "open websocket")
	}

	err = ws.SubscribePublic(c.Args)
	if err != nil {
		return errors.Wrap(err, "subscribe")
	}

	printer(ws.Events())

	return nil
}

func ticker(c *cli.Command) (err error) {
	cl, err := cexio.New(c.String("key"), []byte(c.String("secret")))
	if err != nil {
		return errors.Wrap(err, "new client")
	}

	ws, err := cl.Websocket(context.Background())
	if err != nil {
		return errors.Wrap(err, "open websocket")
	}

	ev, err := ws.Ticker(pair(c.String("pair")))
	if err != nil {
		return errors.Wrap(err, "request")
	}

	t := ev.Data.(*cexio.Ticker)

	tlog.Printw("ticker", "ticker", t, "ts", time.Unix(0, t.Timestamp))

	return nil
}

func balance(c *cli.Command) (err error) {
	cl, err := cexio.New(c.String("key"), []byte(c.String("secret")))
	if err != nil {
		return errors.Wrap(err, "new client")
	}

	ws, err := cl.Websocket(context.Background())
	if err != nil {
		return errors.Wrap(err, "open websocket")
	}

	ev, err := ws.GetBalance()
	if err != nil {
		return errors.Wrap(err, "request")
	}

	b := ev.Data.(*cexio.Balance)

	nonzero := map[string]decimal.Decimal{}
	for t, a := range b.Balances {
		if a.IsZero() {
			continue
		}

		nonzero[t] = a
	}

	ononzero := map[string]decimal.Decimal{}
	for t, a := range b.OrderBalances {
		if a.IsZero() {
			continue
		}

		ononzero[t] = a
	}

	tlog.Printw("balance", "balances", nonzero, "order_balances", ononzero, "ts", time.Unix(0, b.Timestamp))

	return nil
}

func orderbook(c *cli.Command) (err error) {
	cl, err := cexio.New(c.String("key"), []byte(c.String("secret")))
	if err != nil {
		return errors.Wrap(err, "new client")
	}

	ws, err := cl.Websocket(context.Background())
	if err != nil {
		return errors.Wrap(err, "open websocket")
	}

	err = ws.SubscribeOrderbook(pair(c.String("pair")))
	if err != nil {
		return errors.Wrap(err, "subscribe")
	}

	printer(ws.Events())

	return nil
}

func orders(c *cli.Command) (err error) {
	cl, err := cexio.New(c.String("key"), []byte(c.String("secret")))
	if err != nil {
		return errors.Wrap(err, "new client")
	}

	ws, err := cl.Websocket(context.Background())
	if err != nil {
		return errors.Wrap(err, "open websocket")
	}

	ev, err := ws.Orders(pair(c.String("pair")))
	if err != nil {
		return errors.Wrap(err, "request")
	}

	l := ev.Data.([]cexio.OrderStatus)

	for i, o := range l {
		tlog.Printw("open orders", "idx", i, "order", o)
	}

	if len(l) == 0 {
		tlog.Printw("no open orders")
	}

	return nil
}

func orderPlace(c *cli.Command) (err error) {
	cl, err := cexio.New(c.String("key"), []byte(c.String("secret")))
	if err != nil {
		return errors.Wrap(err, "new client")
	}

	ws, err := cl.Websocket(context.Background())
	if err != nil {
		return errors.Wrap(err, "open websocket")
	}

	o := cexio.Order{
		Action: c.String("action"),
		Pair:   c.String("pair"),
	}

	err = o.Price.UnmarshalText([]byte(c.String("price")))
	if err != nil {
		return errors.Wrap(err, "price")
	}

	err = o.Amount.UnmarshalText([]byte(c.String("amount")))
	if err != nil {
		return errors.Wrap(err, "amount")
	}

	ev, err := ws.PlaceOrder(o)
	if err != nil {
		return errors.Wrap(err, "request")
	}

	s := ev.Data.(*cexio.OrderStatus)

	tlog.Printw("place order", "status", s)

	return nil
}

func orderCancel(c *cli.Command) (err error) {
	cl, err := cexio.New(c.String("key"), []byte(c.String("secret")))
	if err != nil {
		return errors.Wrap(err, "new client")
	}

	ws, err := cl.Websocket(context.Background())
	if err != nil {
		return errors.Wrap(err, "open websocket")
	}

	if c.Args.Len() == 0 {
		return errors.New("args expected")
	}

	go printer(ws.Events())

	for _, id := range c.Args {
		ev, err := ws.CancelOrder(id)
		if err != nil {
			return errors.Wrap(err, "request")
		}

		s := ev.Data.(*cexio.OrderCancelled)

		tlog.Printw("cancel order", "id", id, "status", s)
	}

	return nil
}

func printer(evs <-chan cexio.Event) {
	for {
		ev := <-evs

		if tlog.If("include") && tlog.If(ev.Event) ||
			tlog.If("exclude") && !tlog.If(ev.Event) ||
			!tlog.If("include") && !tlog.If("exclude") {
			tlog.Printw("event", "event", ev.Event, "data", ev.Data, "data_type", tlog.FormatNext("%T"), ev.Data)
		}

		switch ev.Data.(type) {
		case *cexio.MarketDataUpdate:
		default:
			//	panic(fmt.Sprintf("%T", ev.Data))
		}
	}
}

func pair(t string) (s1, s2 string) {
	p := strings.Index(t, ":")

	return t[:p], t[p+1:]
}
