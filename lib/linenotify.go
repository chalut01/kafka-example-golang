package lib

import (
	"strings"

	"github.com/utahta/go-linenotify"
)

func Linenotify(msgs string) string {
	msg := "message=" + msgs
	msg = strings.Replace(msg, " ", "-", -1)
	token := "Bearer OoeNUGioVM5rzvnDQvXOpOIqqVD2vUkpxRyGeeRIZAF" // EDIT THIS
	c := linenotify.New()
	c.Notify(token, "msg", "", "", nil)

}
