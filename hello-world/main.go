package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/apigatewaymanagementapi"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	ddb       *dynamodb.DynamoDB
	apigw     *apigatewaymanagementapi.ApiGatewayManagementApi
	tableName = os.Getenv("TABLE_NAME")
)

type Message struct {
	Action       string `json:"action"`
	ConnectionID string `json:"connectionId"`
	Content      string `json:"content"`
	Timestamp    string `json:"timestamp"`
}

func init() {
	sess := session.Must(session.NewSession())
	ddb = dynamodb.New(sess)
	// apigw = apigatewaymanagementapi.New(sess, aws.NewConfig().WithEndpoint(os.Getenv("APIGATEWAY_MANAGEMENT_API")))
	// apigw = apigatewaymanagementapi.New(sess, aws.NewConfig().WithEndpoint("https://wv8xjz3nfl.execute-api.ap-northeast-1.amazonaws.com/test"))
	apigwEndpoint := os.Getenv("APIGATEWAY_MANAGEMENT_API")
	apigw = apigatewaymanagementapi.New(sess, aws.NewConfig().WithEndpoint(apigwEndpoint))
}

func handler(ctx context.Context, req events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	log.Printf("Received event: %+v", req)            // 受信イベント全体をログに出力
	log.Printf("Received request body: %s", req.Body) // 受信したリクエストボディをログに出力

	if req.RequestContext.EventType == "CONNECT" {
		return handleConnect(ctx, req)
	}
	if req.RequestContext.EventType == "DISCONNECT" {
		return handleDisconnect(ctx, req)
	}

	var msg Message
	if err := json.Unmarshal([]byte(req.Body), &msg); err != nil {
		log.Printf("Failed to unmarshal message: %s", err)
		return events.APIGatewayProxyResponse{StatusCode: 400}, err
	}

	log.Printf("Parsed message action: %s", msg.Action)             // 解析したアクションをログに出力
	log.Printf("Parsed message content: %s", msg.Content)           // 解析したコンテンツをログに出力
	log.Printf("Parsed message timestamp: %s", msg.Timestamp)       // 解析したタイムスタンプをログに出力
	log.Printf("Parsed message connectionId: %s", msg.ConnectionID) // 解析したコネクションIDをログに出力

	switch msg.Action {
	case "connect":
		return handleConnect(ctx, req)
	case "message":
		return handleMessage(ctx, req)
	case "disconnect":
		return handleDisconnect(ctx, req)
	default:
		return handleDefault(ctx, req)
	}
}

func handleConnect(ctx context.Context, req events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	connectionID := req.RequestContext.ConnectionID

	// connectionIDをチェック
	log.Printf("ConnectionID: %s", connectionID)

	input := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]*dynamodb.AttributeValue{
			"connectionId": {
				S: aws.String(connectionID),
			},
		},
	}

	_, err := ddb.PutItem(input)
	if err != nil {
		log.Printf("Failed to add connection: %s", err)
		return events.APIGatewayProxyResponse{StatusCode: 500}, err
	}

	return events.APIGatewayProxyResponse{StatusCode: 200}, nil
}

func handleMessage(ctx context.Context, req events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	log.Printf("handleMessage: Received message: %s", req.Body)
	log.Printf("check point 1")

	var msg Message
	if err := json.Unmarshal([]byte(req.Body), &msg); err != nil {
		log.Printf("Failed to unmarshal message: %s", err)
		return events.APIGatewayProxyResponse{StatusCode: 400}, err
	}

	log.Printf("check point 2")

	msg.Timestamp = time.Now().Format(time.RFC3339)
	msg.ConnectionID = req.RequestContext.ConnectionID

	input := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}
	result, err := ddb.Scan(input)
	if err != nil {
		log.Printf("Failed to scan connections: %s", err)
		return events.APIGatewayProxyResponse{StatusCode: 500}, err
	}

	log.Printf("check point 3")

	messageData, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Failed to marshal message: %s", err)
		return events.APIGatewayProxyResponse{StatusCode: 500}, err
	}

	log.Printf("check point 4")

	for _, item := range result.Items {
		log.Println("check point 4.1")
		connID := item["connectionId"].S
		log.Println("check point 4.2")
		postToConnectionInput := &apigatewaymanagementapi.PostToConnectionInput{
			ConnectionId: connID,
			Data:         messageData,
		}
		log.Println("check point 4.3")
		_, err = apigw.PostToConnection(postToConnectionInput)
		if err != nil {
			log.Printf("check point 4.4")
			log.Printf("Failed to post to connection %s: %s", *connID, err)
		}
		log.Println("check point 4.5")
	}

	log.Printf("check point 5")

	return events.APIGatewayProxyResponse{StatusCode: 200}, nil
}

func handleDisconnect(ctx context.Context, req events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	connectionID := req.RequestContext.ConnectionID
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"connectionId": {
				S: aws.String(connectionID),
			},
		},
	}

	_, err := ddb.DeleteItem(input)
	if err != nil {
		log.Printf("Failed to delete connection: %s", err)
		return events.APIGatewayProxyResponse{StatusCode: 500}, err
	}

	return events.APIGatewayProxyResponse{StatusCode: 200}, nil
}

func handleDefault(ctx context.Context, req events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	log.Printf("Received unknown action: %v", req)
	return events.APIGatewayProxyResponse{StatusCode: 200}, nil
}

func main() {
	lambda.Start(handler)
}
