// +build integration

package parsepush_test

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/daaku/parsepush"
	"github.com/facebookgo/ensure"
	"github.com/facebookgo/parse"
)

var (
	integrationApplicationID = defaultEnv(
		"PARSE_APP_ID", "spAVcBmdREXEk9IiDwXzlwe0p4pO7t18KFsHyk7j")
	integrationRestAPIKey = defaultEnv(
		"PARSE_REST_API_KEY", "t6ON64DfTrTL4QJC322HpWbhN6fzGYo8cnjVttap")
)

func defaultEnv(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func uuid(t testing.TB) string {
	u := make([]byte, 16)
	_, err := rand.Read(u[:])
	ensure.Nil(t, err)
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:])
}

func TestIntegrate(t *testing.T) {
	// we use github.com/facebookgo/parse for the API interactions
	client := parse.Client{
		Credentials: parse.RestAPIKey{
			ApplicationID: integrationApplicationID,
			RestAPIKey:    integrationRestAPIKey,
		},
	}

	// push data is described at
	// https://www.parse.com/docs/push_guide#options/REST
	givenPushData := map[string]interface{}{"answer": "42"}

	// create new installation
	installationID := uuid(t)
	ir := map[string]string{
		"installationId": installationID,
		"deviceType":     "embedded",
	}
	res := make(map[string]string)
	_, err := client.Post(&url.URL{Path: "/1/installations"}, ir, &res)
	ensure.Nil(t, err, "creating installation")
	installationObjectID := res["objectId"]

	// start push receiver connection
	pushes := make(chan []byte)
	conn, err := parsepush.NewConn(
		parsepush.ConnApplicationID(integrationApplicationID),
		parsepush.ConnInstallationID(installationID),
		parsepush.ConnPushHandler(func(p []byte) {
			pushes <- p
		}),
		parsepush.ConnErrorHandler(func(err error) {
			t.Fatal("unexpected error:", err)
		}),
	)
	ensure.Nil(t, err)

	// obvious integration test downside
	time.Sleep(2 * time.Second)

	// send push
	pushReq := map[string]interface{}{
		"where": map[string]string{"objectId": installationObjectID},
		"data":  givenPushData,
	}
	_, err = client.Post(&url.URL{Path: "/1/push"}, pushReq, nil)
	ensure.Nil(t, err, "sending push")

	// wait for push to arrive
	payload := <-pushes
	close(pushes)
	push := make(map[string]interface{})
	ensure.Nil(t, json.Unmarshal(payload, &push))
	ensure.Subset(t, push, givenPushData)

	// close our push receiver and clean-up associated resources
	ensure.Nil(t, conn.Close())
}
