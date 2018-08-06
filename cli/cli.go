package dhtchord

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"myDHT"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

// Chord : implement
type Chord struct {
	nP       *mydht.MyNode
	sP       *rpc.Server
	listener net.Listener
	port     string
	debug    string
}

// Localaddr : addr
func (C *Chord) Localaddr() string {
	return fmt.Sprintf("%v:%v", C.nP.Addr, C.port)
}

// NewCli start
func (C *Chord) NewCli() {
	if C.port == "" {
		C.port = "3410"
	}
	C.nP = mydht.NewNode(C.port)
	C.sP = rpc.NewServer()
	//listen
	C.sP.Register(C.nP)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", C.port))
	if err != nil {
		log.Fatal("fail to create a server: listen error", err)
	}
	go C.sP.Accept(lis)
	//C.sP.HandleHTTP("/abc", "/debug")
	mydht.Start(C.nP)
	C.listener = lis
	//go http.Serve(lis, nil)
}

func fetchInput() ([]string, error) {
	input := bufio.NewReader(os.Stdin)
	com, err := input.ReadString('\n')
	if err == nil {
		return strings.Split(strings.TrimSpace(com), " "), nil
	}
	return []string{}, err
}

func (C *Chord) helpCommand(input ...string) error {
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

// PortCommand : change port
func (C *Chord) PortCommand(input ...string) error {
	if C.nP != nil || C.sP != nil {
		return errors.New("a ring has already been created or joined")
	}
	if len(input) == 0 {
		return errors.New("lack parameter")
	}
	C.port = input[0]
	//fmt.Printf("port has been set to %v\n", C.port)
	return nil
}

// CreateCommand : create : create a new ring
func (C *Chord) CreateCommand(input ...string) error {
	if C.nP != nil || C.sP != nil {
		return errors.New("a ring has already been created or joined")
	}
	C.NewCli()
	fmt.Printf("a ring successfully creates containing a node at address %v:%v\n", C.nP.Addr, C.nP.Port)
	return nil
}

// JoinCommand : join <address> : join an existing ring with a node at <address>
func (C *Chord) JoinCommand(input ...string) error {
	if C.nP != nil || C.sP != nil {
		return errors.New("a ring has already been created or joined")
	}

	C.NewCli()
	//time.Sleep(10 * time.Second)
	err := mydht.Join(C.nP, input[0])
	if err != nil {
		return err
	}
	fmt.Printf("%v join a ring with a node at address %v\n", C.nodeAddr(), input[0])
	time.Sleep(500 * time.Millisecond)

	return nil
}

func (C *Chord) nodeAddr() string {
	return fmt.Sprintf("%v:%v", C.nP.Addr, C.nP.Port)
}

// QuitCommand : quit : shut down
func (C *Chord) QuitCommand(input ...string) error {
	err := mydht.RPCCheckFail(C.nP.Successor[0])
	if err != nil {
		time.Sleep(1 * time.Second)
	}
	if C.nodeAddr() == C.nP.Successor[0] {
		fmt.Println("cannot reserve data : no succcessor")
		C.listener.Close()
		C.nP.Closed = true
		fmt.Printf("%v quit\n", C.nodeAddr())
		C.nP = nil
		C.sP = nil
		time.Sleep(500 * time.Millisecond)
		return nil
	}
	//fmt.Println(C.nodeAddr(), " ", C.nP.Successor[0])
	err = mydht.RPCCheckFail(C.nP.Successor[0])
	for err != nil {
		time.Sleep(500 * time.Millisecond)
		err = mydht.RPCCheckFail(C.nP.Successor[0])
	}
	err = mydht.RPCReceiveData(C.nP.Successor[0], C.nP.Data)
	if err != nil {
		fmt.Println("here", err)
		C.listener.Close()
		C.nP.Closed = true
		fmt.Printf("%v quit\n", C.nodeAddr())
		C.nP = nil
		C.sP = nil
		time.Sleep(500 * time.Millisecond)
		return nil
	}
	C.listener.Close()
	C.nP.Closed = true
	fmt.Printf("%v quit\n", C.nodeAddr())
	C.nP = nil
	C.sP = nil
	time.Sleep(500 * time.Millisecond)
	return nil
}

// GetCommand : get <key> : find the given key in the currently active ring
func (C *Chord) GetCommand(input ...string) (string, error) {
	var nodeAddr string
	err := C.nP.FindSuccessor(mydht.HashString(input[0]), &nodeAddr)
	for err != nil {
		time.Sleep(500 * time.Millisecond)
		err = C.nP.FindSuccessor(mydht.HashString(input[0]), &nodeAddr)
	}
	if nodeAddr == "" {
		fmt.Printf("%v ", nodeAddr)
		return "", errors.New("fail to find successor of key")
	}
	//fmt.Println(nodeAddr)
	client, err := rpc.Dial("tcp", nodeAddr)
	if err != nil {
		fmt.Printf("%v ", nodeAddr)
		return "", err
	}
	defer client.Close()
	var val string
	err = client.Call("MyNode.Get", input[0], &val)
	if err != nil {
		fmt.Printf("%v ", nodeAddr)
		return "", err
	}
	fmt.Printf("<%v, %v> in node %v\n", input[0], val, nodeAddr)
	return val, nil
}

// PutCommand : put <key> <value> : insert the given key and value into the ring
func (C *Chord) PutCommand(input ...string) error {
	//fmt.Println("start put")
	//fmt.Printf("node %v\n", nP.NodeAddr())
	var nodeAddr string
	err := C.nP.FindSuccessor(mydht.HashString(input[0]), &nodeAddr)
	for err != nil {
		time.Sleep(500 * time.Millisecond)
		err = C.nP.FindSuccessor(mydht.HashString(input[0]), &nodeAddr)
	}
	if nodeAddr == "" {
		return errors.New("fail to find successor of key")
	}
	//fmt.Println(nodeAddr)
	client, err := rpc.Dial("tcp", nodeAddr)
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

// GetRandomString : work as the name
func GetRandomString(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

func (C *Chord) putrandomCommand(input ...string) error {
	n, err := strconv.Atoi(input[0])
	var cnt = n
	if err != nil {
		return errors.New("put random fail: fetch parameter fail")
	}
	for n > 0 {
		n--
		key := GetRandomString(5)
		val := GetRandomString(5)
		err := C.PutCommand(key, val)
		if err != nil {
			cnt--
			println(err)
		}
	}
	fmt.Printf("put %v random <key, val>\n", cnt)
	return nil
}

// DeleteCommand : delete <key> : the peer deletes it from the ring.
func (C *Chord) DeleteCommand(input ...string) error {
	var nodeAddr string
	err := C.nP.FindSuccessor(mydht.HashString(input[0]), &nodeAddr)
	for err != nil {
		time.Sleep(500 * time.Millisecond)
		err = C.nP.FindSuccessor(mydht.HashString(input[0]), &nodeAddr)
	}
	if nodeAddr == "" {
		return errors.New("fail to find successor of key")
	}
	client, err := rpc.Dial("tcp", nodeAddr)
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

// DumpCommand : for convenient
func (C *Chord) DumpCommand(input ...string) error {
	arg, reply := 1, false
	return C.nP.Dump(arg, &reply)
}

/*
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
}*/

/*
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
}*/
