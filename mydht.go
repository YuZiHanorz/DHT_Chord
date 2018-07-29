package mydht

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
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
}

//NewNode : a new Node
func NewNode(p string) *MyNode {
	addr := getLocalAddress()

	return &MyNode{
		Addr: addr,
		Port: p,
		ID:   HashString(fmt.Sprintf("%v:%v", addr, p)),
		Data: make(map[string]string),
	}
}

func (n *MyNode) nodeAddr() string {
	return fmt.Sprintf("%v:%v", n.Addr, n.Port)
}

func (n *MyNode) closestPrecedingNode(id *big.Int) string {
	for i := 160; i >= 1; i-- {
		if between(n.ID, HashString(n.FingerTable[i]), id, false) {
			return n.FingerTable[i]
		}
	}
	for i := 5; i >= 0; i-- {
		if between(n.ID, HashString(n.Successor[i]), id, false) {
			return n.Successor[i]
		}
	}
	return n.nodeAddr()
}

//FindSuccessor : just as what the name tells
func (n *MyNode) FindSuccessor(id *big.Int, reply *string) error {
	if between(n.ID, id, HashString(n.Successor[0]), true) {
		*reply = n.Successor[0]
		return nil
	}
	/*var err error
	err = RPCCheckFail(n.Successor[0])
	if err != nil {
		time.Sleep(1200 * time.Millisecond)
	}
	*reply, err = RPCFindSuccessor(n.Successor[0], id)
	return err*/

	presuc := n.closestPrecedingNode(id)
	err := RPCCheckFail(presuc)
	for err != nil {
		time.Sleep(1200 * time.Millisecond)
		presuc = n.closestPrecedingNode(id)
		err = RPCCheckFail(presuc)
	}
	*reply, err = RPCFindSuccessor(presuc, id)
	return err
}

//FindPredecessor : just as what the name tells
func (n *MyNode) FindPredecessor(arg int, reply *string) error {
	*reply = n.Predecessor
	return nil
}

// ReceiveData : Receive a map(from another node)
func (n *MyNode) ReceiveData(data map[string]string, reply *bool) error {
	for key, val := range data {
		n.Data[key] = val
	}
	*reply = true
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
	n.Successor[0] = successorAd
	return nil
}

// Dump : display information
func (n *MyNode) Dump(arg int, reply *bool) error {
	fmt.Printf(`Addr: %v
ID: %v
Successor: %v
Predecessor: %v
Data: %v
Fingers: %v
`, n.nodeAddr(), n.ID, n.Successor, n.Predecessor, n.Data, n.FingerTable)
	return nil
}

// RPCFindSuccessor : call a node to find successor for id
func RPCFindSuccessor(nodeAddr string, id *big.Int) (string, error) {
	if nodeAddr == "" {
		return "", errors.New("RPC address fail")
	}
	client, err := rpc.DialHTTP("tcp", nodeAddr)
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
	client, err := rpc.DialHTTP("tcp", nodeAddr)
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
	client, err := rpc.DialHTTP("tcp", nodeAddr)
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

//Pair : <key, val> (for rpc)
type Pair struct {
	Key, Val string
}

//Put : insert <key, val> to node_n
func (n *MyNode) Put(p Pair, reply *bool) error {
	n.Data[p.Key] = p.Val
	*reply = true
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
	delete(n.Data, key)
	return nil
}

//Notify : notify node_n the existence of node_arg
func (n *MyNode) Notify(arg string, reply *bool) error {
	*reply = false
	if n.Predecessor == "" || between(HashString(n.Predecessor), HashString(arg), n.ID, false) {
		n.Predecessor = arg
		*reply = true
	}
	return nil
}

//GetSucList : get node_n's successor list
func (n *MyNode) GetSucList(arg int, reply *([5]string)) error {
	*reply = n.Successor
	return nil
}

//RPCNotify : call a node to notify the existence of preNode
func RPCNotify(nodeAddr string, preNode string) error {
	if nodeAddr == "" {
		return errors.New("RPC address fail")
	}
	client, err := rpc.DialHTTP("tcp", nodeAddr)
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
	client, err := rpc.DialHTTP("tcp", nodeAddr)
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
	for i := 0; i < 5; i++ {
		err = RPCCheckFail(n.Successor[i])
		if err == nil {
			suc = n.Successor[i]
			break
		}
	}
	if err != nil {
		fmt.Println(err)
		return
	}
	var pre string
	pre, err = RPCFindPredecessor(suc)
	if err == nil {
		if between(n.ID, HashString(pre), HashString(suc), false) {
			suc = pre
		}
	}
	n.Successor[0] = suc
	var sucList [5]string
	sucList, err = RPCGetSucList(suc)
	if err != nil {
		return
	}
	for i := 0; i < 4; i++ {
		n.Successor[i+1] = sucList[i]
	}

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
	s, err := RPCFindSuccessor(n.nodeAddr(), pos)
	if s == "" {
		if err != nil {
			//println(err)
		}
		return
	}
	err = RPCCheckFail(s)
	if err != nil {
		return
	}
	n.FingerTable[n.Next] = s
}

// RPCCheckFail : check if node is fail
func RPCCheckFail(nodeAddr string) error {
	if nodeAddr == "" {
		return errors.New("Node fail : address is empty")
	}
	client, err := rpc.DialHTTP("tcp", nodeAddr)
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
		n.Predecessor = ""
	}
}

// StabalizePeriodically : run stablize() per second
func (n *MyNode) stabalizePeriodically(m *sync.Mutex) {
	ticker := time.Tick(1 * time.Second)
	for {
		select {
		case <-ticker:
			m.Lock()
			n.stabalize()
			m.Unlock()
		}
	}
}

// CheckPredecessorPeriodically : run checkPredecessor() per second
func (n *MyNode) checkPredecessorPeriodically(m *sync.Mutex) {
	ticker := time.Tick(1 * time.Second)
	for {
		select {
		case <-ticker:
			m.Lock()
			n.checkPredecessor()
			m.Unlock()
		}
	}
}

// FixFingersPeriodically : run FixFingers() per 0.1 second
func (n *MyNode) fixFingersPeriodically(m *sync.Mutex) {
	ticker := time.Tick(60 * time.Millisecond)
	for {
		select {
		case <-ticker:
			m.Lock()
			n.fixFingers()
			m.Unlock()
		}
	}
}

//MyServer type
type MyServer struct {
	NodeP       *MyNode
	Listener    net.Listener
	IsListening bool
}

//NewServer : a new Server
func NewServer(n *MyNode) *MyServer {
	return &MyServer{
		NodeP:       n,
		IsListening: false,
	}
}

//Start : start new node
func Start(n *MyNode) {
	for i := 0; i < 5; i++ {
		n.Successor[i] = n.nodeAddr()
	}
	var m sync.Mutex
	go n.fixFingersPeriodically(&m)
	go n.checkPredecessorPeriodically(&m)
	go n.stabalizePeriodically(&m)
}

//Join : for join
func Join(n *MyNode, ad string) error {
	err := n.join(ad)
	return err
}
