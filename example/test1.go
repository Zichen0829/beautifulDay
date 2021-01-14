package main

import (
	"database/sql"
	"fmt"
	"github.com/goburrow/modbus"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func main() {

	//打开数据库
	db, err := sql.Open("sqlite3", "./test.db")
	checkErr(err)

	//向数据库device表内新增数据
	stmtDevice, err := db.Prepare("replace into device(name,ip,port,id)values(?,?,?,?)")
	if err != nil {
		log.Println(err)
	}
	stmtDevice.Exec("device2", "127.0.0.3", "502", "2")
	stmtDevice.Exec("device3", "127.0.0.2", "502", "3")

	//删除表内字段
	/*
	/_, err1 := db.Exec("delete from device where name=?", "device2")
	if err1 != nil {
		fmt.Println("exec failed, ", err)
		return
	}

	 */

	//向数据库tags表内新增数据
	/*stmtTags, err := db.Prepare("replace into tags(name,type,addr,device)values(?,?,?,?)")
	if err != nil {
		log.Println(err)
	}
	stmtTags.Exec("tag3", 1, 10001, "device1")
	stmtTags.Exec("tag4", 0, 10003, "device3")
	stmtTags.Exec("tag5", 9, 40003, "device3")

	 */


	//删除tags表内字段

	/*_, err1 := db.Exec("delete from tags where name=?", "tag5")
	if err1 != nil {
		fmt.Println("exec failed, ", err)
		return
	}

	 */


	//创建结构,接收device表内容
	type deviceInfo struct {
		name string   `db:"name"`
		ip string     `db:"ip"`
		port string   `db:"port"`
		id string     `db:"id"`
	}


	//从device表中查询
	rowsDevice,err:=db.Query("SELECT * FROM device")
	address := make (map[string]string) //存储设备name和tcp地址
	for rowsDevice.Next(){
		var device deviceInfo

		err=rowsDevice.Scan(&device.name,&device.ip,&device.port,&device.id)
		checkErr(err)

		fmt.Println("Device information:", device)

		//get TCP address
		address[device.name] = device.ip + ":" + device.port

	}

	fmt.Printf("All device and address:\n%v\n",address)

	rowsDevice.Close()


	//创建结构,接收tags表内容
	type tagsInfo struct {
		name string    `db:"name"`
		tagsType int8  `db:"type"`
		addr uint16    `db:"addr"` //改了类型,为了getResult函数内调用方便
		device string  `db:"device"`
	}


	//从tags表中查询
	rowsTags,err:=db.Query("SELECT * FROM tags")

	//mainMap := make(map[string]map[string][]uint16)
	//subMap := make (map[string][]uint16)
	//deviceAddressTags := make (map[[string][]string) //存储device的name和address，某device对应的所有tag
	//result := make (map[string]string) //存储tags的name和对应读取结果

	tagsDevice := make (map[string][]string) //存储device和该device对应的所有tags
	tagsAllInfo := make (map[string][]uint16) //存储tags表内的name、tagsType、addr
	for rowsTags.Next(){
		var tags tagsInfo

		err=rowsTags.Scan(&tags.name,&tags.tagsType,&tags.addr,&tags.device)
		checkErr(err)
		fmt.Println("tags:", tags, tags.device)
		//fmt.Println("1111",address[tags.device])
		//fmt.Println("999999999999999%T\n",tags.addr)

		tagsDevice[tags.device] = append(tagsDevice[tags.device],tags.name)
		tagsAllInfo[tags.name] = append(tagsAllInfo[tags.name],uint16(tags.tagsType),tags.addr)

		//subMap[tags.name] = append(subMap[tags.name],uint16(tags.tagsType),tags.addr)
		//mainMap[tags.device] = append(mainMap[tags.device],subMap)
		//fmt.Printf("tagsType: %v    tagsAddr: %v\n",tagsAllInfo[tags.name][0],tagsAllInfo[tags.name][1] )


	}
	rowsTags.Close()

	//fmt.Printf("这里：：：：\n%v\n%T\n",tagsDevice,tagsDevice)
	//fmt.Printf("here：：：：\n%v\n%T\n",tagsAllInfo,tagsAllInfo)

	//开始测试连通性并获得和存储结果
	finalResult := make (database)
	for device, _ := range tagsDevice {
		//fmt.Printf("%v !!!!!!!!!!!!!!!!!! %v\n%T   %T\n",device,allTags,device,allTags)
		fmt.Printf("*************  Linking  %v  %v  **************\n",device, address[device])

		var tcpFailedTimes int
	LOOP:_, err := net.Dial("tcp", address[device])
		if err != nil {
			tcpFailedTimes ++ //如果TCP连接失败，失败次数加1
			fmt.Printf("fail time(s): %v\n", tcpFailedTimes)
			fmt.Printf("Connect %v %v failed：%v\n", device, address[device],time.Now())
			time.Sleep(3 * time.Millisecond) //延迟5ms进行下一次TCP连接
			if tcpFailedTimes < 5 { //失败次数小于5次，则继续连接
				goto LOOP

			}else { //连接5次失败后，停止TCP该设备，直接对设备下所有点信息赋值nil
				tcpFailedTimes = 0  //失败次数清空，待用
				for _, tagsName := range tagsDevice[device] { //同设备下所有点存nil
					//saveResult(tagsName, "nil")
					finalResult[tagsName] = "nil"
					fmt.Printf("tagsName: %v  result: %v \n",tagsName, finalResult[tagsName])
				}

				//os.Exit(1) 直接退出程序
			}
		}else { //设备连接成功，同设备下所有的点读取点信息并存储到DB内
			fmt.Printf("Link Succeed and Save Data！\n")

			for _, tagsName := range tagsDevice[device] { //连接失败，同设备下所有点存nil
				result := getResult(address[device],tagsAllInfo[tagsName][0],tagsAllInfo[tagsName][1] )
				//saveResult(tagsName, result)
				finalResult[tagsName] = result
				fmt.Printf("tagsName: %v  result: %v \n", tagsName, finalResult[tagsName])
			}

		}


	}

	http.ListenAndServe("localhost:8080", finalResult)

	db.Close() //关闭DB


}


//error函数
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}


//tagsType函数，根据tags 的type不同取值进行不同的操作,同时返回某点连接正常后查询的结果
func getResult(addr string, tagsType uint16, tagsAddr uint16) (result string){

	//connectTcp(addr) //验证TCP是否可以连接

	cli := modbus.TCPClient(addr) //连接TCP

	//查找对应类型
	switch {
	case tagsType == 0:
		if tagsAddr > 10000 && tagsAddr < 20000 {
			res, err := cli.ReadCoils(tagsAddr - 1,1) //tagsAddr 改成uint16类型
			//fmt.Printf("%T\n",res)
			checkErr(err)
			result = strconv.FormatUint(uint64(res[0]), 10)
		}else{
			result = "nil"
		}

	case tagsType == 9:
		if tagsAddr > 40000 && tagsAddr < 50000 {
			res, err := cli.ReadHoldingRegisters(tagsAddr - 40001,4)
			//fmt.Println(res, res[0],res[1])
			checkErr(err)

			for i :=0; i< 8; i++ {
				result += strconv.FormatUint(uint64(res[i]), 16) + " " //10进制
			}

			result= strings.Trim(result," ")
		}else{
			result = "nil"
		}

	default:
		result = "nil"
		//fmt.Printf("failed")
	}

	return
}


//测试TCP连通性,并且存值
/*var tcpFailedTimes int
func connectTcp(device,addr string) {
	_, err := net.Dial("tcp", addr)
	if err != nil {
		tcpFailedTimes ++
		fmt.Printf("fail time(s): %v\n", tcpFailedTimes)
		fmt.Printf("Connect %v %v failed：%v\n", device, addr,time.Now())
		time.Sleep(5 * time.Second)
		if tcpFailedTimes < 10 {
			connectTcp(device, addr)
		}else { //连接10次失败后存值
			tcpFailedTimes = 0
			fmt.Printf("failed time(s): %v\n", tcpFailedTimes)
			fmt.Printf("stop!!!!!!先存nil，接着运行呀！")
			//saveResult()
			//os.Exit(1) 直接退出程序
		}
	}else {
		fmt.Printf("这里的意思就是成功呀，啥也不用管！")
		//getResult()
		//saveResult()
	}
}

 */


//存储更新后的点数据到DB内
func saveResult(name, data string) {
	//打开数据库
	db, err := sql.Open("sqlite3", "./result.db")
	checkErr(err)

	//创建表格
	/*
		stmt, err := db.Prepare("CREATE TABLE result (name VARCHAR (32) DEFAULT '' NOT NULL, data VARCHAR (128) DEFAULT '' NOT NULL,PRIMARY KEY (name));")
		checkErr(err)
		_, err1 := stmt.Exec()
		checkErr(err1)

	*/

	//更新表内数据
	//向数据库result表内新增数据
	stmtDevice, err := db.Prepare("replace into result(name,data)values(?,?)")
	if err != nil {
		log.Println(err)
	}
	stmtDevice.Exec(name, data)

	//创建结构,接收result表内容
	type resultInfo struct {
		name string   `db:"name"`
		data string   `db:"data"`
	}

	//从result表中查询
	rows,err:=db.Query("SELECT * FROM result")
	allData := make(database) //存储写入DB内的tags的名称和结果
	fmt.Println("Final showed results:")
	for rows.Next(){
		var result resultInfo
		err= rows.Scan(&result.name,&result.data)
		checkErr(err)
		fmt.Printf("tagsName: %v  tagsResult: %v \n", result.name,result.data)
		allData[result.name] = result.data
	}


	http.ListenAndServe("localhost:8080", allData)

	//time.Sleep(1 * time.Second)

	/* var srv = http.Server{
		Addr: "localhost:8080",
	}
	srv.Shutdown(context.Background())

	 */


	//删除字段
	/* _, err1 := db.Exec("delete from result where name=?", "name")
	if err1 != nil {
		fmt.Println("exec failed, ", err)
		return
	}

	 */

	db.Close()
}


type database map[string]string

func (db database) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	/*for name, data := range allData {
		fmt.Fprintf(w, "%s: %s\n", name, data)
	}

	 */

	switch req.URL.Path {
	case "/resultList":
		for name, data := range db {
			fmt.Fprintf(w, "%s: %s\n", name, data)
		}
	case "/data":
		name := req.URL.Query().Get("name")
		data, ok := db[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound) // 404
			fmt.Fprintf(w, "no such item: %q\n", name)
			return
		}
		fmt.Fprintf(w, "%s\n", data)
	default:
		w.WriteHeader(http.StatusNotFound) // 404
		fmt.Fprintf(w, "no such page: %s\n", req.URL)
	}
}




