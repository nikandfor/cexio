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
	"sync"
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

		mu   sync.Mutex
		reqs map[string]*req
	}

	req struct {
		ev   Event
		errc chan error
	}

	Event struct {
		Event string      `json:"e"`
		OK    string      `json:"ok,omitempty"`
		ReqID string      `json:"oid,omitempty"`
		Data  interface{} `json:"data,omitempty"`

		raw []byte
	}

	TickEvent struct {
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
		Timestamp int64           `json:"timestamp"`
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

	Ticker struct {
		Timestamp          int64           `json:"timestamp,omitempty,string"`
		Low                decimal.Decimal `json:"low,omitempty"`
		High               decimal.Decimal `json:"high,omitempty"`
		Last               decimal.Decimal `json:"last,omitempty"`
		Volume             decimal.Decimal `json:"volume,omitempty"`
		Volume30d          decimal.Decimal `json:"volume30d,omitempty"`
		Bid                decimal.Decimal `json:"bid,omitempty"`
		Ask                decimal.Decimal `json:"ask,omitempty"`
		PriceChange        decimal.Decimal `json:"priceChange,omitempty"`
		PriceChangePercent decimal.Decimal `json:"priceChangePercentage,omitempty"`

		Pair []string `json:"pair,omitempty"`

		Symbol1 string `json:"-"`
		Symbol2 string `json:"-"`
	}

	Balance struct {
		Balances      map[string]decimal.Decimal `json:"balance"`
		OrderBalances map[string]decimal.Decimal `json:"obalance"`
		Timestamp     int64                      `json:"time,omitempty"`
	}

	Order struct {
		Action string          `json:"act"`
		Pair   string          `json:"pair"`
		Price  decimal.Decimal `json:"price"`
		Amount decimal.Decimal `json:"amount"`
	}

	order Order

	OrderStatus struct {
		ID        string          `json:"id"`
		Action    string          `json:"act"`
		Time      int64           `json:"time"`
		Price     decimal.Decimal `json:"price"`
		Amount    decimal.Decimal `json:"amount"`
		Pending   decimal.Decimal `json:"pending"`
		Completed bool            `json:"complete"`
	}

	orderStatus OrderStatus

	OrderCancelled struct {
		ID   string `json:"id"`
		Time int64  `json:"time"`
	}

	orderCancelled OrderCancelled

	Error struct {
		Err string `json:"error"`
	}
)

const (
	Buy  = "buy"
	Sell = "sell"
)

func newWS(key string, secret []byte) *Websocket {
	return &Websocket{
		wsbase: "wss://ws.cex.io/ws",
		key:    key,
		secret: secret,
		c:      make(chan Event),
		reqs:   make(map[string]*req),
	}
}

func (ws *Websocket) Events() <-chan Event {
	return ws.c
}

// Subscribe subscribes to "tickers" (trades) or "pair-A-B" where A and B are BTC, ETH, USD and so on.
func (ws *Websocket) SubscribePublic(tickers []string) (err error) {
	return ws.send(map[string]interface{}{
		"e":     "subscribe",
		"rooms": tickers,
	})
}

func (ws *Websocket) Ticker(s1, s2 string) (ev Event, err error) {
	return ws.req(Event{Event: "ticker", Data: []string{s1, s2}})
}

func (ws *Websocket) GetBalance() (ev Event, err error) {
	return ws.req(Event{Event: "get-balance"})
}

func (ws *Websocket) SubscribeOrderbook(sym1, sym2 string) (err error) {
	return ws.send(Event{
		Event: "order-book-subscribe",
		Data: map[string]interface{}{
			"pair":      []string{sym1, sym2},
			"subscribe": true,
			"depth":     -1,
		},
		ReqID: fmt.Sprintf("obs_%d", time.Now().UnixNano()),
	})
}

func (ws *Websocket) Orders(sym1, sym2 string) (ev Event, err error) {
	return ws.req(Event{
		Event: "open-orders",
		Data: map[string]interface{}{
			"pair": []string{sym1, sym2},
		},
	})
}

func (ws *Websocket) PlaceOrder(o Order) (ev Event, err error) {
	return ws.req(Event{
		Event: "place-order",
		Data:  order(o),
	})
}

func (ws *Websocket) CancelOrder(id string) (ev Event, err error) {
	return ws.req(Event{
		Event: "cancel-order",
		Data: struct {
			OrderID string `json:"order_id"`
		}{
			OrderID: id,
		},
	})
}

func (ws *Websocket) req(m Event) (ev Event, err error) {
	t := time.Now()
	reqid := fmt.Sprintf("%d", t.UnixNano())

	r := &req{
		errc: make(chan error, 1),
	}
	ws.reqs[reqid] = r

	defer func() {
		delete(ws.reqs, reqid)
	}()

	m.ReqID = reqid

	err = ws.send(m)
	if err != nil {
		return ev, errors.Wrap(err, "send")
	}

	err = <-r.errc
	if err != nil {
		return ev, errors.Wrap(err, "response")
	}

	ev = r.ev

	if err, _ = ev.Data.(error); err != nil {
		return ev, err
	}

	return ev, nil
}

func (ws *Websocket) send(m interface{}) (err error) {
	return ws.conn.WriteJSON(m)
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

	ev, err := ws.readEvent(ws.conn)
	if err != nil {
		return errors.Wrap(err, "read auth response")
	}

	err = ws.handleAuth(ev)
	if err != nil {
		return errors.Wrap(err, "auth")
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
		ev, err := ws.readEvent(c)
		if err != nil {
			return err
		}

		switch ev.Event {
		case "ping":
			err = c.WriteJSON(Event{Event: "pong"})
			if err != nil {
				return errors.Wrap(err, "write pong")
			}

			continue
		}

		if ev.ReqID != "" {
			r, ok := ws.reqs[ev.ReqID]

			if ok {
				r.ev = ev
				r.errc <- err

				continue
			}
		}

		ws.c <- ev
	}
}

func (ws *Websocket) readEvent(c *websocket.Conn) (ev Event, err error) {
	var tp int
	var p []byte

	tp, p, err = c.ReadMessage()
	if err != nil {
		return ev, errors.Wrap(err, "read")
	}

	tlog.V("raw").Printw("message", "tp", tp, "msg", p)

	ev.raw = p

	if tp != websocket.TextMessage {
		return ev, errors.New("not a text message: %x", tp)
	}

	err = json.Unmarshal(p, &ev)
	if err != nil {
		return ev, errors.Wrap(err, "decode e")
	}

	if ev.OK == "error" {
		err = ws.parseError(&ev, p)
		return
	}

	switch ev.Event {
	//	case "connected":
	case "disconnecting":
		return ev, errors.New("disconnected: %v", "") //, r.Reason)
	case "ping":
	case "tick":
		err = ws.parseTick(&ev, p)
	case "ohlcv24":
		err = ws.parseOHLCV(&ev, p)
	case "md":
		err = ws.parseMD(&ev, p)
	case "md_groupped":
		err = ws.parseMDGrouped(&ev, p)
	case "history":
		err = ws.parseHistory(&ev, p)
	case "history-update":
		err = ws.parseHistoryUpdate(&ev, p)
	case "ticker":
		err = ws.parseTicker(&ev, p)
	case "get-balance":
		err = ws.parseBalance(&ev, p)
	case "order-book-subscribe", "md_update":
		err = ws.parseOrderbook(&ev, p)
	case "open-orders":
		err = ws.parseListOrders(&ev, p)
	case "place-order":
		err = ws.parsePlaceOrder(&ev, p)
	case "cancel-order":
		err = ws.parseCancelOrder(&ev, p)
	}

	return
}

func (ws *Websocket) parseTick(ev *Event, p []byte) (err error) {
	ev.Data = &TickEvent{}

	err = json.Unmarshal(p, ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	return nil
}

func (ws *Websocket) parseOHLCV(ev *Event, p []byte) (err error) {
	q := &ohlcv24{}

	ev.Data = q

	err = json.Unmarshal(p, ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	ev.Data = (*OHLCV24)(q)

	return nil
}

func (ws *Websocket) parseMD(ev *Event, p []byte) (err error) {
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

	err = json.Unmarshal(p, ev)
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

	return nil
}

func (ws *Websocket) parseMDGrouped(ev *Event, p []byte) (err error) {
	type q struct {
		ID   int64                               `json:"id"`
		Pair string                              `json:"pair"`
		Buy  map[decimal.Decimal]decimal.Decimal `json:"buy"`
		Sell map[decimal.Decimal]decimal.Decimal `json:"sell"`
	}

	md := &q{}
	ev.Data = md

	err = json.Unmarshal(p, ev)
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

	return nil
}

func (ws *Websocket) parseHistory(ev *Event, p []byte) (err error) {
	h := &hist{}
	ev.Data = &h

	err = json.Unmarshal(p, ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	ev.Data = (*History)(h)

	return nil
}

func (ws *Websocket) parseHistoryUpdate(ev *Event, p []byte) (err error) {
	h := &hist{}
	ev.Data = &h

	err = json.Unmarshal(p, ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	ev.Data = (*History)(h)

	return nil
}

func (ws *Websocket) parseTicker(ev *Event, p []byte) (err error) {
	q := &Ticker{}
	ev.Data = q

	err = json.Unmarshal(p, ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	q.Timestamp *= int64(time.Second)

	q.Symbol1 = q.Pair[0]
	q.Symbol2 = q.Pair[1]

	return nil
}

func (ws *Websocket) parseBalance(ev *Event, p []byte) (err error) {
	q := &Balance{}
	ev.Data = q

	err = json.Unmarshal(p, ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	q.Timestamp *= int64(time.Millisecond)

	return nil
}

func (ws *Websocket) parseOrderbook(ev *Event, p []byte) (err error) {
	type qtp struct {
		Time      int64           `json:"time"`
		Timestamp int64           `json:"timestamp"`
		ID        int64           `json:"id"`
		Pair      string          `json:"pair"`
		Bids      []priceLevel    `json:"bids"`
		Asks      []priceLevel    `json:"asks"`
		BuyTotal  decimal.Decimal `json:"buy_total"`
		SellTotal decimal.Decimal `json:"sell_total"`
	}

	q := &qtp{}
	ev.Data = q

	err = json.Unmarshal(p, ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	sym := strings.SplitN(q.Pair, ":", 2)

	md := &MarketData{
		Timestamp: q.Timestamp,
		ID:        int64(q.ID),
		Pair:      q.Pair,
		BuyTotal:  q.BuyTotal,
		SellTotal: q.SellTotal,
		Symbol1:   sym[0],
		Symbol2:   sym[1],
	}

	if md.Timestamp == 0 {
		md.Timestamp = q.Time
	}

	md.Buy = make([]PriceLevel, len(q.Bids))
	for i, l := range q.Bids {
		md.Buy[i] = (PriceLevel)(l)
	}

	md.Sell = make([]PriceLevel, len(q.Asks))
	for i, l := range q.Asks {
		md.Sell[i] = (PriceLevel)(l)
	}

	ev.Data = md

	return nil
}

func (ws *Websocket) parseListOrders(ev *Event, p []byte) (err error) {
	var q []orderStatus
	ev.Data = &q

	err = json.Unmarshal(p, ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	list := make([]OrderStatus, len(q))

	for i, up := range q {
		list[i] = OrderStatus{
			ID:        up.ID,
			Action:    up.Action,
			Time:      up.Time,
			Price:     up.Price,
			Amount:    up.Amount,
			Pending:   up.Pending,
			Completed: up.Completed,
		}
	}

	ev.Data = list

	return nil
}

func (ws *Websocket) parsePlaceOrder(ev *Event, p []byte) (err error) {
	var up OrderStatus
	ev.Data = (*orderStatus)(&up)

	err = json.Unmarshal(p, ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	return nil
}

func (ws *Websocket) parseCancelOrder(ev *Event, p []byte) (err error) {
	var up OrderCancelled
	ev.Data = (*orderCancelled)(&up)

	err = json.Unmarshal(p, ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	return nil
}

func (ws *Websocket) parseError(ev *Event, p []byte) (err error) {
	ev.Data = &Error{}

	err = json.Unmarshal(p, ev)
	if err != nil {
		return errors.Wrap(err, "unmarshal data")
	}

	return nil
}

func (ws *Websocket) handleAuth(ev Event) (err error) {
	if e := ev.Data.(map[string]interface{})["error"]; e != nil {
		return errors.New("%v", e)
	}
	if ev.OK != "ok" {
		return errors.New("not ok: %s", ev.Raw())
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

func (o *order) UnmarshalJSON(data []byte) (err error) {
	var q struct {
		Type   string          `json:"type"`
		Pair   []string        `json:"pair"`
		Price  decimal.Decimal `json:"price"`
		Amount decimal.Decimal `json:"amount"`
	}

	err = json.Unmarshal(data, &q)
	if err != nil {
		return err
	}

	o.Action = q.Type
	o.Pair = q.Pair[0] + ":" + q.Pair[1]
	o.Price = q.Price
	o.Amount = q.Amount

	return nil
}

func (o order) MarshalJSON() (d []byte, err error) {
	type q struct {
		Type   string          `json:"type"`
		Pair   []string        `json:"pair"`
		Price  decimal.Decimal `json:"price"`
		Amount decimal.Decimal `json:"amount"`
	}

	s1, s2, err := pair(o.Pair)
	if err != nil {
		return nil, errors.Wrap(err, "parse pair")
	}

	return json.Marshal(q{
		Type:   o.Action,
		Pair:   []string{s1, s2},
		Price:  o.Price,
		Amount: o.Amount,
	})
}

func (up *orderStatus) UnmarshalJSON(data []byte) (err error) {
	var q struct {
		ID        string          `json:"id"`
		Time      int64           `json:"time"`
		Type      string          `json:"type"`
		Price     decimal.Decimal `json:"price"`
		Amount    decimal.Decimal `json:"amount"`
		Pending   decimal.Decimal `json:"pending"`
		Completed bool            `json:"complete"`
	}

	err = json.Unmarshal(data, &q)
	if err != nil {
		return err
	}

	up.ID = q.ID
	up.Time = q.Time
	up.Action = q.Type
	up.Price = q.Price
	up.Amount = q.Amount
	up.Pending = q.Pending
	up.Completed = q.Completed

	return nil
}

func (up orderStatus) MarshalJSON() (d []byte, err error) {
	type q struct {
		ID        string          `json:"id"`
		Time      int64           `json:"time"`
		Type      string          `json:"type"`
		Price     decimal.Decimal `json:"price"`
		Amount    decimal.Decimal `json:"amount"`
		Pending   decimal.Decimal `json:"pending"`
		Completed bool            `json:"complete"`
	}

	return json.Marshal(q{
		ID:        up.ID,
		Type:      up.Action,
		Time:      up.Time,
		Price:     up.Price,
		Amount:    up.Amount,
		Pending:   up.Pending,
		Completed: up.Completed,
	})
}

func (up *orderCancelled) UnmarshalJSON(data []byte) (err error) {
	var q struct {
		ID   string `json:"id"`
		Time int64  `json:"time"`
	}

	err = json.Unmarshal(data, &q)
	if err != nil {
		return err
	}

	up.ID = q.ID
	up.Time = q.Time * int64(time.Millisecond)

	return nil
}

func (up orderCancelled) MarshalJSON() (d []byte, err error) {
	type q struct {
		ID   string `json:"id"`
		Time int64  `json:"time"`
	}

	return json.Marshal(q{
		ID:   up.ID,
		Time: up.Time / int64(time.Millisecond),
	})
}

func (e *Error) Error() string { return e.Err }

func pair(t string) (s1, s2 string, err error) {
	p := strings.Index(t, ":")
	if p == -1 {
		return "", "", errors.New("marlormed pair")
	}

	return t[:p], t[p+1:], nil
}
