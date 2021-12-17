package containers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type RabbitAdminErrorResponse struct {
	StatusCode int
	Message    string `json:"error"`
	Reason     string `json:"reason"`
}

func (rme RabbitAdminErrorResponse) Error() string {
	return fmt.Sprintf("Error %d (%s): %s", rme.StatusCode, rme.Message, rme.Reason)
}

type RabbitAdmin struct {
	url *url.URL
}

type GetMessagesRequest struct {
	Vhost    string `json:"vhost"`
	Truncate int    `json:"truncate"`
	AckMode  string `json:"ackmode"`
	Encoding string `json:"encoding"`
	Count    int    `json:"count"`
}

func NewGetMessagesRequest() GetMessagesRequest {
	return GetMessagesRequest{
		Vhost:    "/",
		Truncate: 50000,
		AckMode:  "ack_requeue_false",
		Encoding: "auto",
		Count:    100,
	}
}

func NewRabbitAdmin(adminUrl string) (*RabbitAdmin, error) {
	parsed, err := url.Parse(adminUrl)
	if err != nil {
		return nil, err
	}

	return &RabbitAdmin{parsed}, nil
}

func (admin *RabbitAdmin) Consume(queue string, req GetMessagesRequest) ([]map[string]interface{}, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := admin.doReq(
		"POST",
		"/api/queues/"+url.PathEscape(req.Vhost)+"/"+url.PathEscape(queue)+"/get",
		body,
	)
	if err != nil {
		return nil, err
	}

	//goland:noinspection GoUnhandledErrorResult
	defer resp.Body.Close()

	var respObj []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&respObj); err != nil {
		return nil, err
	}

	return respObj, nil
}

func (admin *RabbitAdmin) Purge(queue string, vhost string) error {
	type purgeRequest struct {
		Vhost string `json:"vhost"`
		Name  string `json:"name"`
		Mode  string `json:"mode"`
	}

	reqBytes, err := json.Marshal(purgeRequest{
		Vhost: vhost,
		Name:  queue,
		Mode:  "purge",
	})
	if err != nil {
		return err
	}

	resp, err := admin.doReq(
		"DELETE",
		"/api/queues/"+url.PathEscape(vhost)+"/"+url.PathEscape(queue)+"/contents",
		reqBytes,
	)
	if err != nil {
		return err
	}

	if resp.Body != nil {
		_ = resp.Body.Close()
	}

	return nil
}

func (admin *RabbitAdmin) doReq(method string, uri string, body []byte) (*http.Response, error) {
	location := *admin.url
	location.User = nil

	req, err := http.NewRequest(method, location.String()+uri, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Close = true
	if pwd, present := admin.url.User.Password(); present {
		req.SetBasicAuth(admin.url.User.Username(), pwd)
	}
	req.Header.Add("Content-Type", "application/json")

	client := http.Client{}
	resp, err := client.Do(req)

	if err = parseResponseError(resp); err != nil {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}

	return resp, err
}

func parseResponseError(res *http.Response) (err error) {
	if res.StatusCode >= http.StatusBadRequest {
		rme := RabbitAdminErrorResponse{}
		if err = json.NewDecoder(res.Body).Decode(&rme); err != nil {
			rme.Message = fmt.Sprintf("Error %d from RabbitMQ: %s", res.StatusCode, err)
		}
		rme.StatusCode = res.StatusCode
		return rme
	}
	return nil
}
