package cexio

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gorilla/websocket"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/tlog"
	"github.com/shopspring/decimal"
)

type (
	Websocket struct {
		wsbase string
		key    string
		secret []byte

		conn *websocket.Conn

		c chan Event
	}

	Event struct {
		Event string      `json:"e"`
		OK    string      `json:"ok,omitempty"`
		OID   string      `json:"oid,omitempty"`
		Data  interface{} `json:"data,omitempty"`

		raw []byte
	}

	Tick struct {
		Symbol1 string          `json:"symbol1"`
		Symbol2 string          `json:"symbol2"`
		Price   decimal.Decimal `json:"price"`

		Open24 decimal.Decimal `json:"open24"`
		Volume decimal.Decimal `json:"volume"`
	}

	OHLCV24 struct {
		Open   decimal.Decimal `json:"open"`
		High   decimal.Decimal `json:"high"`
		Low    decimal.Decimal `json:"low"`
		Close  decimal.Decimal `json:"close"`
		Volume decimal.Decimal `json:"volume"`
	}

	ohlcv24 OHLCV24

	MarketData struct {
		ID        int64           `json:"id"`
		Pair      string          `json:"pair"`
		BuyTotal  decimal.Decimal `json:"buy_total"`
		SellTotal decimal.Decimal `json:"sell_total"`
		Buy       []PriceLevel    `json:"buy"`
		Sell      []PriceLevel    `json:"sell"`

		Symbol1 string `json:"symbol1"`
		Symbol2 string `json:"symbol2"`

		Full bool `json:"-"`
	}

	PriceLevel struct {
		Price  decimal.Decimal `json:"price"`
		Volume decimal.Decimal `json:"volume"`
	}

	priceLevel PriceLevel

	History struct {
		Trades []Trade
	}

	Trade struct {
		Action    string          `json:"act"`
		Timestamp int64           `json:"ts"`
		Amount    decimal.Decimal `json:"amount"`
		Price     decimal.Decimal `json:"price"`
		TxID      int64           `json:"tx_id"`
	}

	hist History
)

func newWS(key string, secret []byte) *Websocket {
	return &Websocket{
		wsbase: "wss://ws.cex.io/ws",
		key:    key,
		secret: secret,
		c:      make(chan Event),
	}
}

func (ws *Websocket) Events() <-chan Event {
	return ws.c
}

func (ws *Websocket) Subscribe(tickers []string) (err error) {
	err = ws.conn.WriteJSON(map[string]interface{}{
		"e":     "subscribe",
		"rooms": tickers,
	})
	if err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func (ws *Websocket) connect(ctx context.Context) (err error) {
	ws.conn, _, err = (&websocket.Dialer{}).DialContext(ctx, ws.wsbase, nil)
	if err != nil {
		return errors.Wrap(err, "dial")
	}

	var r map[string]interface{}
	err = ws.conn.ReadJSON(&r)
	if err != nil {
		return errors.Wrap(err, "read auth")
	}

	if r["e"] != "connected" {
		return errors.New("unexpected connected msg: %v", r)
	}

	ts := time.Now()
	sig := ws.authToken(ts)

	err = ws.conn.WriteJSON(map[string]interface{}{
		"e": "auth",
		"auth": map[string]interface{}{
			"key":       ws.key,
			"signature": sig,
			"timestamp": ts.Unix(),
		},
	})
	if err != nil {
		return errors.Wrap(err, "write auth")
	}

	go func() {
		err := ws.reader(ws.conn)
		if err != nil {
			tlog.Printw("websocket reader stopped", "err", err)
		}
	}()

	return nil
}

func (ws *Websocket) reader(c *websocket.Conn) (err error) {
	for {
		var tp int
		var p []byte

		tp, p, err = c.ReadMessage()
		if err != nil {
			return errors.Wrap(err, "read")
		}

		tlog.V("raw").Printw("message", "tp", tp, "msg", p)

		if tp != websocket.TextMessage {
			return errors.New("not a text message: %x", tp)
		}

		var ev Event
		ev.raw = p

		err = json.Unmarshal(p, &ev)
		if err != nil {
			return errors.Wrap(err, "decode e")
		}

		switch ev.Event {
		//	case "connected":
		case "disconnecting":
			return errors.New("disconnected: %v", "") //, r.Reason)
		case "ping":
			// TODO
			//	err = c.WriteJSON(struct {
			//		E string `json:"e"`
			//	}{E: "pong"})
			if err != nil {
				err = errors.Wrap(err, "write pong")
			}
		case "auth":
			err = ws.handleAuth(ev, p)
		case "tick":
			err = ws.handleTick(ev, p)
		case "ohlcv24":
			err = ws.handleOHLCV(ev, p)
		case "md":
			err = ws.handleMD(ev, p)
		case "md_groupped":
			err = ws.handleMDGrouped(ev, p)
		case "history":
			err = ws.handleHistory(ev, p)
		case "history-update":
			err = ws.handleHistoryUpdate(ev, p)
		default:
			//	tlog.Printw("msg", "e", ev.Event)
			ws.c <- ev
		}

		if err != nil {
			return err
		}
	}
}

func (ws *Websocket) handleTick(ev Event, p []byte) (err error) {
	ev.Data = &Tick{}

	err = json.Unmarshal(p, &ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	ws.c <- ev

	return nil
}

func (ws *Websocket) handleOHLCV(ev Event, p []byte) (err error) {
	q := &ohlcv24{}

	ev.Data = q

	err = json.Unmarshal(p, &ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	ev.Data = (*OHLCV24)(q)

	ws.c <- ev

	return nil
}

func (ws *Websocket) handleMD(ev Event, p []byte) (err error) {
	type qtp struct {
		ID        float64         `json:"id"`
		Pair      string          `json:"pair"`
		BuyTotal  decimal.Decimal `json:"buy_total"`
		SellTotal decimal.Decimal `json:"sell_total"`
		Buy       []priceLevel    `json:"buy"`
		Sell      []priceLevel    `json:"sell"`
	}

	q := &qtp{}
	ev.Data = q

	err = json.Unmarshal(p, &ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	sym := strings.SplitN(q.Pair, ":", 2)

	md := &MarketData{
		ID:        int64(q.ID),
		Pair:      q.Pair,
		BuyTotal:  q.BuyTotal,
		SellTotal: q.SellTotal,
		Symbol1:   sym[0],
		Symbol2:   sym[1],
	}

	md.Buy = make([]PriceLevel, len(q.Buy))
	for i, l := range q.Buy {
		md.Buy[i] = (PriceLevel)(l)
	}

	md.Sell = make([]PriceLevel, len(q.Sell))
	for i, l := range q.Sell {
		md.Sell[i] = (PriceLevel)(l)
	}

	ev.Data = md

	ws.c <- ev

	return nil
}

func (ws *Websocket) handleMDGrouped(ev Event, p []byte) (err error) {
	type q struct {
		ID   int64                               `json:"id"`
		Pair string                              `json:"pair"`
		Buy  map[decimal.Decimal]decimal.Decimal `json:"buy"`
		Sell map[decimal.Decimal]decimal.Decimal `json:"sell"`
	}

	md := &q{}
	ev.Data = md

	err = json.Unmarshal(p, &ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	sym := strings.SplitN(md.Pair, ":", 2)

	conv := &MarketData{
		ID:      int64(md.ID),
		Pair:    md.Pair,
		Buy:     make([]PriceLevel, 0, len(md.Buy)),
		Sell:    make([]PriceLevel, 0, len(md.Sell)),
		Symbol1: sym[0],
		Symbol2: sym[1],
		Full:    true,
	}

	for k, v := range md.Buy {
		conv.Buy = append(conv.Buy, PriceLevel{
			Price:  k,
			Volume: v,
		})
	}

	for k, v := range md.Sell {
		conv.Sell = append(conv.Sell, PriceLevel{
			Price:  k,
			Volume: v,
		})
	}

	sort.Slice(conv.Buy, func(i, j int) bool {
		return conv.Buy[i].Price.GreaterThan(conv.Buy[j].Price)
	})

	sort.Slice(conv.Sell, func(i, j int) bool {
		return conv.Sell[i].Price.LessThan(conv.Sell[j].Price)
	})

	ev.Data = conv

	ws.c <- ev

	return nil
}

func (ws *Websocket) handleHistory(ev Event, p []byte) (err error) {
	h := &hist{}
	ev.Data = &h

	err = json.Unmarshal(p, &ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	ev.Data = (*History)(h)

	ws.c <- ev

	return nil
}

func (ws *Websocket) handleHistoryUpdate(ev Event, p []byte) (err error) {
	h := &hist{}
	ev.Data = &h

	err = json.Unmarshal(p, &ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	ev.Data = (*History)(h)

	ws.c <- ev

	return nil
}

func (ws *Websocket) handleAuth(ev Event, p []byte) (err error) {
	if e := ev.Data.(map[string]interface{})["error"]; e != nil {
		return errors.New("auth: %v", e)
	}
	if ev.OK != "ok" {
		return errors.New("auth: not ok: %s", p)
	}

	return nil
}

func (ws *Websocket) authToken(t time.Time) string {
	h := hmac.New(sha256.New, ws.secret)

	fmt.Fprintf(h, "%d%s", t.Unix(), ws.key)

	sum := h.Sum(nil)

	return hex.EncodeToString(sum)
}

func (ev *Event) Raw() []byte { return ev.raw }

func (pl *priceLevel) UnmarshalJSON(data []byte) (err error) {
	_, tp, _, err := jsonparser.Get(data)
	if err != nil {
		return err
	}

	if tp == jsonparser.Array {
		q := []*decimal.Decimal{
			&pl.Price, &pl.Volume,
		}

		err = json.Unmarshal(data, &q)
		if err != nil {
			return errors.Wrap(err, "as array")
		}

		if len(q) != 2 {
			return errors.New("expected price-volume pair")
		}

		return nil
	}

	return errors.New("unexpected type: %v", tp)
}

func (h *hist) UnmarshalJSON(data []byte) (err error) {
	_, err = jsonparser.ArrayEach(data, func(v []byte, tp jsonparser.ValueType, off int, e error) {
		if err != nil {
			return
		}

		if e != nil {
			err = e
			return
		}

		if tp != jsonparser.String {
			err = errors.New("expected null value")
			return
		}

		seg := bytes.Split(v, []byte(":"))
		if len(seg) != 5 {
			err = errors.New("wrong number of segments")
			return
		}

		if string(seg[0]) != "buy" && string(seg[0]) != "sell" {
			err = errors.New("bad action: %s", seg[0])
			return
		}

		t := Trade{
			Action: string(seg[0]),
		}

		t.Timestamp, err = strconv.ParseInt(string(seg[1]), 10, 64)
		if err != nil {
			err = errors.Wrap(err, "parse timestamp")
			return
		}

		t.Timestamp *= int64(time.Millisecond)

		err = t.Amount.UnmarshalJSON(seg[2])
		if err != nil {
			err = errors.Wrap(err, "parse amount")
			return
		}

		err = t.Price.UnmarshalJSON(seg[3])
		if err != nil {
			err = errors.Wrap(err, "parse price")
			return
		}

		t.TxID, err = strconv.ParseInt(string(seg[4]), 10, 64)
		if err != nil {
			err = errors.Wrap(err, "parse txid")
			return
		}

		h.Trades = append(h.Trades, t)
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *ohlcv24) UnmarshalJSON(data []byte) (err error) {
	q := []*decimal.Decimal{
		&p.Open,
		&p.High,
		&p.Low,
		&p.Close,
		&p.Volume,
	}

	err = json.Unmarshal(data, &q)
	if err != nil {
		return err
	}

	return nil
}
