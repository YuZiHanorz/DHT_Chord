package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"myDHT"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)


var nP *mydht.MyNode
var sP *mydht.MyServer
var port = "3410"

func localaddr() string {
	return fmt.Sprintf("%v:%v", nP.Addr, nP.Port)
}

// NewCli start
func NewCli() {
	nP = mydht.NewNode(port)
	sP = mydht.NewServer(nP)

	//listen
	rpc.Register(nP)
	rpc.HandleHTTP()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", nP.Port))
	if err != nil {
		log.Fatal("fail to create a server: listen error", err)
	}
	mydht.Start(nP)
	sP.Listener = lis
	sP.IsListening = true
	go http.Serve(lis, nil)
}

func fetchInput() ([]string, error) {
	input := bufio.NewReader(os.Stdin)
	com, err := input.ReadString('\n')
	if err == nil {
		return strings.Split(strings.TrimSpace(com), " "), nil
	}
	return []string{}, err
}

func helpCommand(input ...string) error {
	fmt.Println(`Commands:
main commands:
	port <n> : set the port that this node should listen on
	create : create a new ring
	join <address> : join an existing ring with a node at <address>
	quit : shut down
commands for <key, value>:
	put <key> <value> : insert the given key and value into the ring
	putrandom <k> : randomly generate k pairs of <key,value> and put them
	get <key> : find the given key in the currently active ring
	delete <key> : the peer deletes it from the ring.
commands for debug:
	dump : display information about the current node
	dumpkey <key> : find the node resposible for <key>, asks it to dump
	dumpaddr <address> : query a specific host and dump its info
	dumpall : dumping all information in the ring in clockwise order
	`)
	return nil
}

func portCommand(input ...string) error {
	if nP != nil || sP != nil {
		return errors.New("a ring has already been created or joined")
	}
	if len(input) == 0 {
		return errors.New("lack parameter")
	}
	port = input[0]
	fmt.Printf("port has been set to %v\n", port)
	return nil
}

func createCommand(input ...string) error {
	if nP != nil || sP != nil {
		return errors.New("a ring has already been created or joined")
	}
	NewCli()
	fmt.Printf("a ring successfully creates containing a node at address %v:%v\n", nP.Addr, nP.Port)
	return nil
}

func joinCommand(input ...string) error {
	if nP != nil || sP != nil {
		return errors.New("a ring has already been created or joined")
	}
	NewCli()
	err := mydht.Join(nP, input[0])
	if err != nil {
		return err
	}
	fmt.Printf("join a ring containing a node at address %v\n", input[0])
	return nil
}

func nodeAddr() string {
	return fmt.Sprintf("%v:%v", nP.Addr, nP.Port)
}

func quitCommand(input ...string) error {
	err := mydht.RPCCheckFail(nP.Successor[0])
	if err != nil {
		time.Sleep(time.Second)
	}
	if nodeAddr() == nP.Successor[0] {
		fmt.Println("cannot reserve data : no succcessor")
		sP.Listener.Close()
		os.Exit(1)
		return nil
	}
	err = mydht.RPCReceiveData(nP.Successor[0], nP.Data)
	if err != nil {
		fmt.Println(err)
		sP.Listener.Close()
		os.Exit(1)
		return nil
	}
	sP.Listener.Close()
	os.Exit(1)
	return nil
}

func getCommand(input ...string) error {
	var nodeAddr string
	err := nP.FindSuccessor(mydht.HashString(input[0]), &nodeAddr)
	if err != nil {
		return err
	}
	if nodeAddr == "" {
		return errors.New("fail to find successor of key")
	}
	client, err := rpc.DialHTTP("tcp", nodeAddr)
	if err != nil {
		return err
	}
	defer client.Close()
	var val string
	err = client.Call("MyNode.Get", input[0], &val)
	if err != nil {
		return err
	}
	fmt.Printf("<%v, %v> in node %v\n", input[0], val, nodeAddr)
	return nil
}

func putCommand(input ...string) error {
	//fmt.Println("start put")
	//fmt.Printf("node %v\n", nP.NodeAddr())
	var nodeAddr string
	err := nP.FindSuccessor(mydht.HashString(input[0]), &nodeAddr)
	if err != nil {
		return err
	}
	if nodeAddr == "" {
		return errors.New("fail to find successor of key")
	}
	//fmt.Println(nodeAddr)
	client, err := rpc.DialHTTP("tcp", nodeAddr)
	if err != nil {
		return err
	}
	defer client.Close()
	var reply bool
	var arg = mydht.Pair{
		Key: input[0],
		Val: input[1],
	}
	err = client.Call("MyNode.Put", arg, &reply)
	if err != nil {
		return err
	}
	fmt.Printf("put <%v, %v> in node %v\n", input[0], input[1], nodeAddr)
	return nil
}

func getRandomString(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

func putrandomCommand(input ...string) error {
	n, err := strconv.Atoi(input[0])
	var cnt = n
	if err != nil {
		return errors.New("put random fail: fetch parameter fail")
	}
	for n > 0 {
		n--
		key := getRandomString(5)
		val := getRandomString(5)
		err := putCommand(key, val)
		if err != nil {
			cnt--
			println(err)
		}
	}
	fmt.Printf("put %v random <key, val>\n", cnt)
	return nil
}

func deleteCommand(input ...string) error {
	var nodeAddr string
	err := nP.FindSuccessor(mydht.HashString(input[0]), &nodeAddr)
	if err != nil {
		return err
	}
	if nodeAddr == "" {
		return errors.New("fail to find successor of key")
	}
	client, err := rpc.DialHTTP("tcp", nodeAddr)
	if err != nil {
		return err
	}
	defer client.Close()
	var reply bool
	err = client.Call("MyNode.Delete", input[0], &reply)
	if err != nil {
		return err
	}
	fmt.Printf("delete key<%v> in node %v\n", input[0], nodeAddr)
	return nil
}

func dumpCommand(input ...string) error {
	arg, reply := 1, false
	return nP.Dump(arg, &reply)
}

var commandLine = map[string]func(input ...string) error{
	"help":      helpCommand,
	"join":      joinCommand,
	"quit":      quitCommand,
	"port":      portCommand,
	"create":    createCommand,
	"get":       getCommand,
	"put":       putCommand,
	"putrandom": putrandomCommand,
	"delete":    deleteCommand,
	"dump":      dumpCommand,
}

func main() {
	fmt.Println(`Welcome to yzh's dht. 
You can do whatever you want as long as your commands conform to the format.
type "help" for further information.`)

	for {
		input, err := fetchInput()
		if err != nil {
			fmt.Println("Wrong input!")
			continue
		}
		_, haveCommand := commandLine[input[0]]
		if !haveCommand {
			fmt.Println("Wrong command!")
			continue
		}
		err = commandLine[input[0]](input[1:]...)
		if err != nil {
			fmt.Println(err)
		}

	}
}
