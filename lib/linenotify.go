package lib

import (
	"strings"

	"github.com/spf13/viper"
	"github.com/utahta/go-linenotify"
)

func Linenotify(msgs string) string {
	msg := "" + msgs
	msg = strings.Replace(msg, " ", "-", -1)
	viper.SetDefault("app.linetoken", "OoeNUGioVM5rzvnDQvXOpOIqqVD2vUkpxRyGeeRIZAF")
	token := viper.GetString("app.linetoken") // EDIT THIS
	c := linenotify.New()
	resp, err := c.Notify(token, msg, "", "", nil)

	if err != nil {
		return err.Error()
	}
	return resp.Message
}
