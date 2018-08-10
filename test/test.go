package main

import (
	"DHT_improve/cli"
	"fmt"
	"strconv"
)

var cnt int
var cho [56]dhtchord.Chord
var tmp int

func test() {
	//test1
	m := make(map[string]string)

	cho[0].CreateCommand()

	for loop := 0; loop < 5; loop++ {
		for i := 1; i <= 15; i++ {
			tmp = i + loop*10
			port := 3410 + tmp
			p := strconv.Itoa(port)
			cho[tmp].PortCommand(p)
			cho[tmp].JoinCommand(cho[0].Localaddr())
			//time.Sleep(1 * time.Second)
		}

		for i := 0; i < 300; i++ {
			key := dhtchord.GetRandomString(20)
			value := dhtchord.GetRandomString(5)
			cho[0].PutCommand(key, value)
			m[key] = value
		}

		num := 0
		for key, val := range m {
			value, _ := cho[0].GetCommand(key)
			if value != val {
				cnt++
			}
			num++
			if num == 200 {
				break
			}

		}
		num = 0
		for key := range m {
			cho[0].DeleteCommand(key)
			delete(m, key)
			num++
			if num == 150 {
				break
			}
		}

		for i := 11; i <= 15; i++ {
			tmp = i + loop*10
			cho[tmp].QuitCommand()
			//time.Sleep(1 * time.Second)
		}

		for i := 0; i < 300; i++ {
			key := dhtchord.GetRandomString(20)
			value := dhtchord.GetRandomString(5)
			cho[0].PutCommand(key, value)
			m[key] = value
		}

		num = 0
		for key, val := range m {
			value, _ := cho[0].GetCommand(key)
			if value != val {
				cnt++
			}
			num++
			if num == 200 {
				break
			}

		}
		num = 0
		for key := range m {
			cho[0].DeleteCommand(key)
			delete(m, key)
			num++
			if num == 150 {
				break
			}
		}

	}
	if cnt == 0 {
		fmt.Printf("\nPass!\n")
	} else {
		fmt.Printf("mistakes: %v, fail", cnt)
	}

}

/*func test() {
	m := make(map[string]string)
	cho[0].CreateCommand()
	for i := 1; i <= 4; i++ {
		tmp = i
		port := 3410 + tmp
		p := strconv.Itoa(port)
		cho[tmp].PortCommand(p)
		cho[tmp].JoinCommand(cho[0].Localaddr())
		//time.Sleep(1 * time.Second)
	}
	for i := 0; i < 300; i++ {
		key := strconv.Itoa(i)
		value := dhtchord.GetRandomString(5)
		cho[0].PutCommand(key, value)
		m[key] = value
	}
	time.Sleep(20 * time.Second)
	for i := 11; i < 300; i++ {
		key := strconv.Itoa(i)
		cho[0].DeleteCommand(key)
	}

	for i := 5; i <= 7; i++ {
		tmp = i
		port := 3410 + tmp
		p := strconv.Itoa(port)
		cho[tmp].PortCommand(p)
		cho[tmp].JoinCommand(cho[0].Localaddr())
		//time.Sleep(1 * time.Second)
	}
	for i := 3; i <= 7; i++ {
		cho[i].QuitCommand()
	}

	time.Sleep(20 * time.Second)
}*/

func main() {
	test()
}
