package cexio

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWSAuth(t *testing.T) {
	sum := (&Websocket{
		key:    "1WZbtMTbMbo2NsW12vOz9IuPM",
		secret: []byte("1IuUeW4IEWatK87zBTENHj1T17s"),
	}).authToken(time.Unix(1448034533, 0))
	assert.Equal(t, "7d581adb01ad22f1ed38e1159a7f08ac5d83906ae1a42fe17e7d977786fe9694", sum)

	sum = (&Websocket{
		key:    "1WZbtMTbMbo2NsW12vOz9IuPM",
		secret: []byte("1IuUeW4IEWatK87zBTENHj1T17s"),
	}).authToken(time.Unix(1448035135, 0))
	assert.Equal(t, "9a84b70f51ea2b149e71ef2436752a1a7c514f521e886700bcadd88f1767b7db", sum)
}

func unhex(s string) (r []byte) {
	r, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}

	return
}
