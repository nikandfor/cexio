package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	"github.com/nikandfor/cexio"
	"github.com/nikandfor/cli"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/tlog"
	"github.com/nikandfor/tlog/ext/tlflag"
)

func main() {
	cli.App = cli.Command{
		Name:   "cex cli",
		Before: before,
		Flags: []*cli.Flag{
			cli.NewFlag("key", "", "api key"),
			cli.NewFlag("secret", "", "api key secret"),

			cli.NewFlag("log", "stderr+dm", "log destination"),
			cli.NewFlag("v", "", "verbosity topics"),
			cli.NewFlag("debug", "", "debug addr to listen to", cli.Hidden),
		},
		Commands: []*cli.Command{{
			Name:   "test",
			Action: test,
			Args:   cli.Args{},
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

func test(c *cli.Command) (err error) {
	cl, err := cexio.New(c.String("key"), []byte(c.String("secret")))
	if err != nil {
		return errors.Wrap(err, "new client")
	}

	ws, err := cl.Websocket(context.Background())
	if err != nil {
		return errors.Wrap(err, "open websocket")
	}

	err = ws.Subscribe(c.Args)
	if err != nil {
		return errors.Wrap(err, "subscribe")
	}

	evs := ws.Events()

	for {
		ev := <-evs

		tlog.Printw("event", "data_type", tlog.FormatNext("%T"), ev.Data, "event", ev)

		switch ev.Data.(type) {
		case *cexio.Tick:
		case *cexio.MarketData:
		case *cexio.History:
		case *cexio.OHLCV24:
		default:
			panic(fmt.Sprintf("%T", ev.Data))
		}
	}

	return nil
}
