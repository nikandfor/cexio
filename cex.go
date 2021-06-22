package cexio

import (
	"context"

	"github.com/nikandfor/errors"
)

type (
	CEX struct {
		key    string
		secret []byte
	}
)

func New(key string, secret []byte) (c *CEX, err error) {
	c = &CEX{
		key:    key,
		secret: secret,
	}

	return c, nil
}

func (c *CEX) Websocket(ctx context.Context) (ws *Websocket, err error) {
	ws = newWS(c.key, c.secret)

	err = ws.connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "connect")
	}

	return ws, nil
}
