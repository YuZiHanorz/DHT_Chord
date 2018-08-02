package main

import (
	"fmt"
	"math/rand"
	"myDHT/cli"
	"strconv"
	"time"
)

var cnt int
var cho [50]dhtchord.Chord
var key [1500]string
var value [1500]string

func test1() {
	//test1

	for i := 0; i < 1500; i++ {
		key[i] = dhtchord.GetRandomString(15)
		value[i] = dhtchord.GetRandomString(5)
		fmt.Printf("<%v, %v>\n", key[i], value[i])
	}
	cho[0].CreateCommand()
	for i := 1; i < 50; i++ {
		port := 3410 + i
		p := strconv.Itoa(port)
		cho[i].PortCommand(p)
		cho[i].JoinCommand(cho[0].Localaddr())
	}
	time.Sleep(2 * time.Second)

	/*for i := 0; i < 50; i++{
		cho[i].DumpCommand()
	}*/

	for i := 0; i < 1500; i++ {
		a := rand.Intn(50)
		cho[a].PutCommand(key[i], value[i])
	}

	//for i := 0; i < 1500; i++ {
	for j := 0; j < 1500; j++ {
		a := rand.Intn(50)
		val, _ := cho[a].GetCommand(key[j])
		if val != value[j] {
			k := 0
			for ; k < 5; k++ {
				time.Sleep(2 * time.Second)
				val, _ = cho[a].GetCommand(key[j])
				if val == value[j] {
					break
				}
			}
			if k == 5 {
				fmt.Println(cho[a].Localaddr(), " ", key[j], " ", val)
				cnt++
			}
		}
	}
	//}
	//fmt.Println(cnt)
	fmt.Printf("\n\n")
	if cnt == 0 {
		fmt.Println("Put_Get_test pass")
	} else {
		fmt.Println("Put_Get_test fail")
	}

}

func test2() {
	//test2

	for i := 1; i < 50; i += 5 {
		cho[i].QuitCommand()
	}
	time.Sleep(2 * time.Second)
	/*for i := 0; i < 50; i++ {
		if i%5 == 1 {
			continue
		}
		cho[i].DumpCommand()
	}*/

	for j := 0; j < 1500; j++ {
		//a := rand.Intn(50)
		val, _ := cho[0].GetCommand(key[j])
		if val != value[j] {
			k := 0
			for ; k < 5; k++ {
				time.Sleep(2 * time.Second)
				val, _ = cho[0].GetCommand(key[j])
				if val == value[j] {
					break
				}
			}
			if k == 5 {
				fmt.Println(key[j], " ", val)
				cnt++
			}
		}
	}

	//fmt.Println(cnt)
	fmt.Printf("\n\n")
	if cnt == 0 {
		fmt.Println("Quit_Get_test pass")
	} else {
		fmt.Println("Quit_Get_test fail")
	}
}

func test3() {

	for i := 1; i < 50; i += 5 {
		cho[i].JoinCommand(cho[3].Localaddr())
	}

	time.Sleep(2 * time.Second)

	for j := 0; j < 1500; j++ {
		a := rand.Intn(50)
		val, _ := cho[a].GetCommand(key[j])
		if val != value[j] {
			k := 0
			for ; k < 5; k++ {
				time.Sleep(2 * time.Second)
				val, _ = cho[a].GetCommand(key[j])
				if val == value[j] {
					break
				}
			}
			if k == 5 {
				fmt.Println(cho[a].Localaddr(), " ", key[j], " ", val)
				cnt++
			}
		}
	}
	fmt.Printf("\n\n")
	if cnt == 0 {
		fmt.Println("join_Get_test pass")
	} else {
		fmt.Println("join_Get_test fail")
	}
}

func test4() {
	for j := 0; j < 1500; j += 20 {
		a := rand.Intn(50)
		cho[a].DeleteCommand(key[j])
	}
	time.Sleep(2 * time.Second)
	for j := 0; j < 1500; j++ {
		a := rand.Intn(50)
		val, _ := cho[a].GetCommand(key[j])
		if j%20 == 0 {
			if val == "" {
				fmt.Println("true")
			} else {
				fmt.Println("false")
				cnt++
			}
			continue
		}
		if val != value[j] {
			k := 0
			for ; k < 5; k++ {
				time.Sleep(2 * time.Second)
				val, _ = cho[a].GetCommand(key[j])
				if val == value[j] {
					break
				}
			}
			if k == 5 {
				fmt.Println(cho[a].Localaddr(), " ", key[j], " ", val)
				cnt++
			}
		}
	}
	fmt.Printf("\n\n")
	if cnt == 0 {
		fmt.Println("Delete_Get_test pass")
	} else {
		fmt.Println("Delete_Get_test fail")
	}
}

func main() {
	test1()
	fmt.Printf("\n\n")
	test2()
	fmt.Printf("\n\n")
	test3()
	fmt.Printf("\n\n")
	test4()
	fmt.Printf("\n\n")
}
