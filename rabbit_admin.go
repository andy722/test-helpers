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

type QueueInfo struct {
	Name      string                 `json:"name"`
	Type      string                 `json:"type,omitempty"`
	Vhost     string                 `json:"vhost,omitempty"`
	Durable   bool                   `json:"durable"`
	Arguments map[string]interface{} `json:"arguments"`

	Messages               int   `json:"messages,omitempty"`
	MessagesReady          int   `json:"messages_ready,omitempty"`
	MessagesUnacknowledged int   `json:"messages_unacknowledged,omitempty"`
	ActiveConsumers        int64 `json:"active_consumers,omitempty"`
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
		"/queues/"+url.PathEscape(req.Vhost)+"/"+url.PathEscape(queue)+"/get",
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
		"/queues/"+url.PathEscape(vhost)+"/"+url.PathEscape(queue)+"/contents",
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

func (admin *RabbitAdmin) GetQueue(queue string, vhost string) (*QueueInfo, error) {
	resp, err := admin.doGet(
		"/queues/" + url.PathEscape(vhost) + "/" + url.PathEscape(queue),
	)
	if err != nil {
		return nil, err
	}

	//goland:noinspection GoUnhandledErrorResult
	defer resp.Body.Close()

	var respObj = QueueInfo{}
	if err := json.NewDecoder(resp.Body).Decode(&respObj); err != nil {
		return nil, err
	}

	return &respObj, nil
}

func (admin *RabbitAdmin) Publish(rk string, vhost string, body string) error {
	type publishRequestProperties struct {
		DeliveryMode int                    `json:"delivery_mode"`
		Headers      map[string]interface{} `json:"headers"`
	}

	type publishRequest struct {
		Vhost           string                   `json:"vhost"`
		Name            string                   `json:"name"`
		Properties      publishRequestProperties `json:"properties"`
		RoutingKey      string                   `json:"routing_key"`
		DeliveryMode    string                   `json:"delivery_mode"`
		Payload         string                   `json:"payload"`
		Headers         map[string]interface{}   `json:"headers"`
		Props           map[string]interface{}   `json:"props"`
		PayloadEncoding string                   `json:"payload_encoding"`
	}

	reqBytes, err := json.Marshal(publishRequest{
		Vhost: vhost,
		Name:  "amq.default",
		Properties: publishRequestProperties{
			DeliveryMode: 2,
			Headers:      map[string]interface{}{},
		},
		RoutingKey:      rk,
		DeliveryMode:    "2",
		Payload:         body,
		Headers:         map[string]interface{}{},
		Props:           map[string]interface{}{},
		PayloadEncoding: "string",
	})
	if err != nil {
		return err
	}

	resp, err := admin.doReq(
		"POST",
		"/exchanges/"+url.PathEscape(vhost)+"/amq.default/publish",
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

func (admin *RabbitAdmin) doGet(uri string) (*http.Response, error) {
	location := *admin.url
	location.User = nil

	req, err := http.NewRequest("GET", location.String()+uri, nil)
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
