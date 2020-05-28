package lib

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strings"
)

func line(msgs string) string {
	msg := msgs
	msg = strings.Replace(msg, " ", "-", -1)
	url := "https://notify-api.line.me/api/notify"
	var jsonStr = []byte(msg)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Authorization", "Bearer OoeNUGioVM5rzvnDQvXOpOIqqVD2vUkpxRyGeeRIZAF")
	//    req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	rstring := string(body)
	return rstring

}
