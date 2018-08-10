package mydht

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// HashString : just as what you think
func HashString(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

const keySize = sha1.Size * 8

var two = big.NewInt(2)
var hashMod = new(big.Int).Exp(two, big.NewInt(keySize), nil)

//find node_id + 2 ^ (fingerentry - 1)
func jump(address string, fingerentry int) *big.Int {
	n := HashString(address)
	fingerentryminus1 := big.NewInt(int64(fingerentry) - 1)
	jump := new(big.Int).Exp(two, fingerentryminus1, nil)
	sum := new(big.Int).Add(n, jump)

	return new(big.Int).Mod(sum, hashMod)
}

//decide whether start < elt <= end(inclusive:true)
func between(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	}
	return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
}

func getLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
}

//Op : operation (for buffer)
type Op struct {
	kind int
	key  string
	val  string
}

//MyNode type
type MyNode struct {
	Addr        string
	Port        string
	Data        map[string]string
	ID          *big.Int
	Successor   [5]string
	Predecessor string
	FingerTable [161]string
	Next        int
	SucMutex    sync.Mutex
	PreMutex    sync.Mutex
	FingerMutex sync.Mutex
	Closed      bool
	buf         *bytes.Buffer
	Fp          *os.File
	BufMutex    sync.Mutex
}

//NewNode : a new Node
func NewNode(p string) *MyNode {
	addr := getLocalAddress()

	return &MyNode{
		Addr: addr,
		Port: p,
		ID:   HashString(fmt.Sprintf("%v:%v", addr, p)),
		Data: make(map[string]string),
		buf:  new(bytes.Buffer),
	}
}

func (n *MyNode) nodeAddr() string {
	return fmt.Sprintf("%v:%v", n.Addr, n.Port)
}

func (n *MyNode) closestPrecedingNode(id *big.Int) string {
	var err error
	n.FingerMutex.Lock()
	defer n.FingerMutex.Unlock()

	for i := 160; i >= 1; i-- {
		if n.FingerTable[i] == "" {
			continue
		}
		if between(n.ID, HashString(n.FingerTable[i]), id, false) {
			/*if id.Cmp(HashString("192.168.127.138:3413")) == 0 {
				fmt.Println(n.FingerTable)
				fmt.Println(n.FingerTable[140])
			}*/
			err = RPCCheckFail(n.FingerTable[i])
			if err != nil {
				continue
			}
			return n.FingerTable[i]
		}
	}

	n.SucMutex.Lock()
	defer n.SucMutex.Unlock()

	for i := 4; i >= 0; i-- {
		if n.Successor[i] == "" {
			continue
		}
		if between(n.ID, HashString(n.Successor[i]), id, false) {
			err = RPCCheckFail(n.Successor[i])
			if err != nil {
				continue
			}
			return n.Successor[i]
		}
	}

	return n.nodeAddr()
}

//FindSuccessor : just as what the name tells
func (n *MyNode) FindSuccessor(id *big.Int, reply *string) error {
	err := RPCCheckFail(n.Successor[0])
	for err != nil {
		time.Sleep(1 * time.Second)
		err = RPCCheckFail(n.Successor[0])
	}

	n.SucMutex.Lock()

	if between(n.ID, id, HashString(n.Successor[0]), true) {
		*reply = n.Successor[0]
		n.SucMutex.Unlock()
		return nil
	}
	n.SucMutex.Unlock()

	/*var err error
	err = RPCCheckFail(n.Successor[0])
	if err != nil {
		time.Sleep(1200 * time.Millisecond)
	}
	*reply, err = RPCFindSuccessor(n.Successor[0], id)
	return err*/

	presuc := n.closestPrecedingNode(id)
	/*err = RPCCheckFail(presuc)
	for err != nil {
		time.Sleep(1 * time.Second)
		presuc = n.closestPrecedingNode(id)
		err = RPCCheckFail(presuc)
		if n.nodeAddr() == "192.168.127.139:3410" && id.Cmp(jump(n.nodeAddr(), 158)) == 0 {
			fmt.Println(presuc, " ", err)
		}
	}*/

	*reply, err = RPCFindSuccessor(presuc, id)
	return err
}

//FindPredecessor : just as what the name tells
func (n *MyNode) FindPredecessor(arg int, reply *string) error {

	n.PreMutex.Lock()
	defer n.PreMutex.Unlock()

	*reply = n.Predecessor
	return nil
}

// ReceiveData : Receive a map(from another node)
func (n *MyNode) ReceiveData(data map[string]string, reply *bool) error {
	n.BufMutex.Lock()
	defer n.BufMutex.Unlock()
	for key, val := range data {
		n.buf.WriteString("P")
		n.buf.WriteString(" ")
		n.buf.WriteString(key)
		n.buf.WriteString(" ")
		n.buf.WriteString(val)
		n.buf.WriteString(" ")
		n.Data[key] = val
	}
	*reply = true
	return nil
}

// CheckData : check if some data shoule be sent
func (n *MyNode) CheckData(nodeAddr string, reply *map[string]string) error {

	n.BufMutex.Lock()
	defer n.BufMutex.Unlock()
	for key, val := range n.Data {
		if between(HashString(nodeAddr), HashString(key), HashString(n.nodeAddr()), true) {
			continue
		}
		(*reply)[key] = val
		n.buf.WriteString("D")
		n.buf.WriteString(" ")
		n.buf.WriteString(key)
		n.buf.WriteString(" ")
		delete(n.Data, key)
	}
	return nil
}

// CheckFail : check if n fails
func (n *MyNode) CheckFail(arg int, reply *bool) error {
	*reply = true
	return nil
}

func (n *MyNode) join(ad string) error {
	successorAd, err := RPCFindSuccessor(ad, HashString(n.nodeAddr()))
	if err != nil {
		return errors.New("join fail : fail to find successor")
	}

	n.SucMutex.Lock()

	n.Successor[0] = successorAd

	n.SucMutex.Unlock()
	//fmt.Println(n.nodeAddr(), " ", n.Successor)
	time.Sleep(500 * time.Millisecond)
	m, err := RPCCheckData(successorAd, n.nodeAddr())
	if err != nil {
		return errors.New("fail to send data")
	}
	n.BufMutex.Lock()
	for key, val := range m {
		n.buf.WriteString("P")
		n.buf.WriteString(" ")
		n.buf.WriteString(key)
		n.buf.WriteString(" ")
		n.buf.WriteString(val)
		n.buf.WriteString(" ")
		n.Data[key] = val
	}
	n.BufMutex.Unlock()
	return nil
}

// Dump : display information
func (n *MyNode) Dump(arg int, reply *bool) error {
	n.SucMutex.Lock()
	n.PreMutex.Lock()
	n.FingerMutex.Lock()
	defer n.SucMutex.Unlock()
	defer n.PreMutex.Unlock()
	defer n.FingerMutex.Unlock()

	fmt.Printf(`Addr: %v
ID: %v
Successor: %v
Predecessor: %v
Data: %v
`, n.nodeAddr(), n.ID, n.Successor, n.Predecessor, n.Data)
	return nil
}

// RPCFindSuccessor : call a node to find successor for id
func RPCFindSuccessor(nodeAddr string, id *big.Int) (string, error) {
	if nodeAddr == "" {
		return "", errors.New("RPC address fail")
	}

	client, err := rpc.Dial("tcp", nodeAddr)
	if err != nil {
		return "", err
	}
	defer client.Close()
	var reply string
	err = client.Call("MyNode.FindSuccessor", id, &reply)
	return reply, err
}

//FindSuccessor : find the successor of id
/*func FindSuccessor(start string, id *big.Int) (string, error) {
	i, maxSteps := 0, 160
	found, nextNode := errors.New("not found"), start
	//fmt.Println("start find successor")
	for found != nil && i <= maxSteps {
		//fmt.Printf("time : %v\n", i)
		nextNode, found = RPCFindSuccessor(nextNode, id)

		if nextNode == "" {
			return "", errors.New("fail to find the successor of id")
		}
		i++
	}
	if found == nil {
		//fmt.Println("find node")
		return nextNode, nil
	}
	return "", errors.New("fail to find the successor of id")
}*/

// RPCFindPredecessor : call a node to return its predecessor
func RPCFindPredecessor(nodeAddr string) (string, error) {
	if nodeAddr == "" {
		return "", errors.New("RPC address fail")
	}
	client, err := rpc.Dial("tcp", nodeAddr)
	if err != nil {
		return "", err
	}
	defer client.Close()
	var reply string
	var arg int
	err = client.Call("MyNode.FindPredecessor", arg, &reply)
	if err != nil {
		return "", err
	}
	if reply == "" {
		return "", errors.New("predecessor fail")
	}
	return reply, nil
}

// RPCReceiveData : call a node to receive data
func RPCReceiveData(nodeAddr string, data map[string]string) error {
	if nodeAddr == "" {
		return errors.New("RPC address fail")
	}
	client, err := rpc.Dial("tcp", nodeAddr)
	if err != nil {
		return err
	}
	defer client.Close()
	var reply bool
	err = client.Call("MyNode.ReceiveData", data, &reply)
	if err != nil {
		return err
	}
	if reply == false {
		return errors.New("send data fail")
	}
	return nil

}

// RPCCheckData : for join
func RPCCheckData(nodeAddr string, pre string) (map[string]string, error) {
	m := make(map[string]string)
	if nodeAddr == "" {
		return m, errors.New("RPC address fail")
	}
	client, err := rpc.Dial("tcp", nodeAddr)
	if err != nil {
		return m, err
	}
	defer client.Close()
	err = client.Call("MyNode.CheckData", pre, &m)
	return m, err
}

//Pair : <key, val> (for rpc)
type Pair struct {
	Key, Val string
}

//Put : insert <key, val> to node_n
func (n *MyNode) Put(p Pair, reply *bool) error {
	n.BufMutex.Lock()
	n.Data[p.Key] = p.Val
	*reply = true
	n.buf.WriteString("P")
	n.buf.WriteString(" ")
	n.buf.WriteString(p.Key)
	n.buf.WriteString(" ")
	n.buf.WriteString(p.Val)
	n.buf.WriteString(" ")
	n.BufMutex.Unlock()
	return nil
}

//Get : find the given key in node_n
func (n *MyNode) Get(key string, reply *string) error {
	val, ok := n.Data[key]
	if ok != true {
		return errors.New("key does not exist")
	}
	*reply = val
	return nil
}

//Delete : delete the given key in node_n
func (n *MyNode) Delete(key string, reply *bool) error {
	_, ok := n.Data[key]
	*reply = ok
	if ok != true {
		return errors.New("key does not exist")
	}
	n.BufMutex.Lock()
	n.buf.WriteString("D")
	n.buf.WriteString(" ")
	n.buf.WriteString(key)
	n.buf.WriteString(" ")
	delete(n.Data, key)
	n.BufMutex.Unlock()
	return nil
}

//Notify : notify node_n the existence of node_arg
func (n *MyNode) Notify(arg string, reply *bool) error {

	n.PreMutex.Lock()
	defer n.PreMutex.Unlock()

	*reply = false
	if n.Predecessor == "" || between(HashString(n.Predecessor), HashString(arg), n.ID, false) {
		n.Predecessor = arg
		*reply = true
	}
	return nil
}

//GetSucList : get node_n's successor list
func (n *MyNode) GetSucList(arg int, reply *([5]string)) error {

	n.SucMutex.Lock()
	defer n.SucMutex.Unlock()

	*reply = n.Successor
	return nil
}

//RPCNotify : call a node to notify the existence of preNode
func RPCNotify(nodeAddr string, preNode string) error {

	if nodeAddr == "" {
		return errors.New("RPC address fail")
	}
	client, err := rpc.Dial("tcp", nodeAddr)
	if err != nil {
		return err
	}
	defer client.Close()
	var reply bool

	err = client.Call("MyNode.Notify", preNode, &reply)
	if err != nil {
		return err
	}
	return nil
}

// RPCGetSucList : Get SuccessorList
func RPCGetSucList(nodeAddr string) ([5]string, error) {
	if nodeAddr == "" {
		return [5]string{}, errors.New("RPC address fail")
	}
	client, err := rpc.Dial("tcp", nodeAddr)
	if err != nil {
		return [5]string{}, err
	}
	defer client.Close()
	var arg int
	var reply [5]string
	err = client.Call("MyNode.GetSucList", arg, &reply)
	if err != nil {
		return [5]string{}, err
	}
	return reply, nil
}

func (n *MyNode) stabalize() {

	var err error
	var suc string

	n.SucMutex.Lock()
	for i := 0; i < 5; i++ {
		err = RPCCheckFail(n.Successor[i])
		if err != nil {
			continue
		}

		suc = n.Successor[i]

		break
	}
	n.SucMutex.Unlock()

	if err != nil {
		fmt.Println(err)
		return
	}
	var pre string
	pre, err = RPCFindPredecessor(suc)
	err1 := RPCCheckFail(pre)
	if err == nil && err1 == nil {
		if between(n.ID, HashString(pre), HashString(suc), false) {
			//fmt.Println("hahaha", n.nodeAddr(), " ", pre, " ", suc)
			suc = pre
		}
	}
	n.SucMutex.Lock()
	n.Successor[0] = suc
	n.SucMutex.Unlock()
	var sucList [5]string
	sucList, err = RPCGetSucList(suc)

	if err != nil {

		return
	}
	n.SucMutex.Lock()
	for i := 0; i < 4; i++ {
		n.Successor[i+1] = sucList[i]
	}
	n.SucMutex.Unlock()

	err = RPCNotify(n.Successor[0], n.nodeAddr())
	if err != nil {
		//fmt.Println(err)
	}
}

func (n *MyNode) fixFingers() {

	n.Next++
	if n.Next > 160 {
		n.Next = 1
	}

	pos := jump(n.nodeAddr(), n.Next)
	var s string

	err := n.FindSuccessor(pos, &s)

	if s == "" {
		if err != nil {
			//println(err)
		}
		return
	}

	n.FingerMutex.Lock()
	//n.FingerTable[n.Next] = s

	for between(n.ID, pos, HashString(s), true) == true {

		/*if n.nodeAddr() == "192.168.127.139:3410" {
			fmt.Println("here2", " ", n.Next, " ", s)
		}*/
		n.FingerTable[n.Next] = s
		n.Next++
		if n.Next > 160 {
			n.Next = 0
			break
		}
		pos = jump(n.nodeAddr(), n.Next)
	}
	if n.Next != 0 {
		n.Next--
	}

	n.FingerMutex.Unlock()
}

// RPCCheckFail : check if node is fail
func RPCCheckFail(nodeAddr string) error {

	if nodeAddr == "" {
		return errors.New("Node fail : address is empty")
	}
	client, err := rpc.Dial("tcp", nodeAddr)

	if err != nil {
		return err
	}
	defer client.Close()
	var args int
	var reply bool
	err = client.Call("MyNode.CheckFail", args, &reply)
	return err
}

func (n *MyNode) checkPredecessor() {

	err := RPCCheckFail(n.Predecessor)
	if err != nil {

		n.PreMutex.Lock()

		n.Predecessor = ""

		n.PreMutex.Unlock()
	}
}

// StabalizePeriodically : run stablize() per second
func (n *MyNode) stabalizePeriodically() {

	ticker := time.Tick(500 * time.Millisecond)
	for {
		if n.Closed == true {
			return
		}
		select {
		case <-ticker:
			n.stabalize()
		}
	}
}

// CheckPredecessorPeriodically : run checkPredecessor() per second
func (n *MyNode) checkPredecessorPeriodically() {

	ticker := time.Tick(500 * time.Millisecond)
	for {
		if n.Closed == true {
			return
		}
		select {
		case <-ticker:
			n.checkPredecessor()
		}
	}
}

// FixFingersPeriodically : run FixFingers() per 0.1 second
func (n *MyNode) fixFingersPeriodically() {

	ticker := time.Tick(500 * time.Millisecond)
	for {
		if n.Closed == true {
			return
		}
		select {
		case <-ticker:
			n.fixFingers()
		}
	}
}

// dataTransfer : tranfer data from memory to file
func (n *MyNode) dataTransfer() {
	if n.Closed == true {
		return
	}
	n.BufMutex.Lock()
	s := n.buf.String()
	if s == "" {
		n.BufMutex.Unlock()
		return
	}
	pos, _ := n.Fp.Seek(0, os.SEEK_END)
	_, err := n.Fp.WriteAt([]byte(s), pos)
	if err != nil {
		fmt.Printf("%v dateTransfer fail\n", n.nodeAddr())
		fmt.Println(err)
		n.BufMutex.Unlock()
		return
	}
	n.buf.Reset()
	n.BufMutex.Unlock()
}

func (n *MyNode) dataTransfePeriodically() {
	ticker := time.Tick(20 * time.Second)
	for {
		if n.Closed == true {
			return
		}
		select {
		case <-ticker:
			n.dataTransfer()
		}
	}
}

//MyServer type
/*type MyServer struct {
	NodeP       *MyNode
	Server		*rpc.Server
}

//NewServer : a new Server
func NewServer(n *MyNode) *MyServer {
	return &MyServer{
		NodeP:       n,
	}
}*/

//Start : start new node
func Start(n *MyNode) {
	for i := 0; i < 5; i++ {
		n.Successor[i] = n.nodeAddr()
	}
	go n.fixFingersPeriodically()
	go n.checkPredecessorPeriodically()
	go n.stabalizePeriodically()
	go n.dataTransfePeriodically()
}

//Join : for join
func Join(n *MyNode, ad string) error {
	err := n.join(ad)
	return err
}
