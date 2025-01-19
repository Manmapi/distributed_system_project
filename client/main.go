package main

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"time"
)

type SetKeyArgs struct {
	BucketName string
	Key        int
	Value      string
}

type GetKeyArgs struct {
	BucketName string
	Key        int
}
type Response struct {
	Data    string
	Message string
}

type EmptyRequest struct{}

type DeleteKeyArgs struct {
	BucketName string
	Key        int
}

func getKey(client *rpc.Client, args GetKeyArgs) string {
	var reply Response
	err := client.Call("Server.GetKey", args, &reply)
	if err != nil {
		log.Println("Has error while trying to get key", err)
	}
	if reply.Message != "" {
		return "Can not get key: " + reply.Message
	}
	data := reply.Data
	return data
}

func getStoreInfo(client *rpc.Client) string {
	var reply Response
	err := client.Call("Server.GetStoreInfo", EmptyRequest{}, &reply)
	if err != nil {
		log.Println("Has error while trying to get key", err)
	}
	if reply.Message != "" {
		log.Println("Has error while trying to get key", reply.Message)
	}
	data := reply.Data
	return data
}

func setKey(client *rpc.Client, args SetKeyArgs) {
	var reply Response
	err := client.Call("Server.SetKey", args, &reply)
	if err != nil {
		log.Println("Has error while trying to set key", err)
	}
	if reply.Message != "OK" {
		log.Println("Has error while trying to set key", reply.Message)
	}
}

func deleteKey(client *rpc.Client, args DeleteKeyArgs) {
	var reply Response
	err := client.Call("Server.DeleteKey", args, &reply)
	if err != nil {
		log.Println("Has error while trying to get key ", err)
	}
}

func testSetGet(client *rpc.Client) {
	setKey(client, SetKeyArgs{"User", 1, "Tran Quang Duy"})
	setKey(client, SetKeyArgs{"StudentId", 1, "27C12027"})
	setKey(client, SetKeyArgs{"DOB", 1, "06/05/2001"})
	name := getKey(client, GetKeyArgs{"User", 1})
	studentId := getKey(client, GetKeyArgs{"StudentId", 1})
	dateOfBirth := getKey(client, GetKeyArgs{"DOB", 1})
	println(name)
	println(studentId)
	println(dateOfBirth)
}

func testMassSetAndGet(client *rpc.Client, bucketName string, count int) {
	startTime := time.Now()

	for i := 0; i < count; i++ {
		setKey(client, SetKeyArgs{
			BucketName: bucketName,
			Key:        i,
			Value:      "Item " + strconv.Itoa(i),
		})
	}

	for i := 0; i < count; i++ {
		getKey(client, GetKeyArgs{
			BucketName: bucketName,
			Key:        i,
		})
	}

	elapsed := time.Since(startTime)
	fmt.Printf("Mass set/delete of %d keys took %v\n", count, elapsed)
}

func testDelete(client *rpc.Client) {
	setKey(client, SetKeyArgs{"User", 2, "Name To Delete"})
	setKey(client, SetKeyArgs{"StudentId", 2, "27C120XX"})
	setKey(client, SetKeyArgs{"DOB", 2, "DD/MM/YYYY"})
	deleteKey(client, DeleteKeyArgs{"User", 2})
	name := getKey(client, GetKeyArgs{"User", 2})
	studentId := getKey(client, GetKeyArgs{"StudentId", 2})
	dateOfBirth := getKey(client, GetKeyArgs{"DOB", 2})
	println(name)
	println(studentId)
	println(dateOfBirth)
}
func main() {
	client, err := rpc.DialHTTP("tcp", "localhost"+":2233")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	println("Test simple set and get")
	println("----------------------------------------------")
	testSetGet(client)
	println("----------------------------------------------")

	println("Test Delete")
	println("----------------------------------------------")
	testDelete(client)
	println("----------------------------------------------")

	println("Test Mass/Bulk Set and Get")
	println("----------------------------------------------")
	testMassSetAndGet(client, "BulkBucket", 10000)
	println("----------------------------------------------")

	println("Get store info")
	println("----------------------------------------------")
	println(getStoreInfo(client))
	println("----------------------------------------------")
}
