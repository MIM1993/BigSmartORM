/*
@Time : 2021/12/5 下午2:33
@Author : MuYiMing
@File : engine
@Software: GoLand
*/
package BigSmartORM

import (
	"database/sql"
	"errors"
	"reflect"
	"runtime"

	"strconv"
	"strings"
)

//mysql Engine 创建核心引擎，最终要求实现Enginer接口
type BigSmartEngine struct {
	Db           *sql.DB
	TableName    string
	Prepare      string
	AllExec      []interface{}
	Sql          string
	WhereParam   string
	LimitParam   string
	OrderParam   string
	OrWhereParam string
	WhereExec    []interface{}
	UpdateParam  string
	UpdateExec   []interface{}
	FieldParam   string
	TransStatus  int
	Tx           *sql.Tx
	GroupParam   string
	HavingParam  string
}

//设置表名
func (bse *BigSmartEngine) Table(name string) *BigSmartEngine {
	bse.TableName = name

	//重置引擎
	//bse.resetSmallormEngine()
	return bse
}

//获取表名
func (bse *BigSmartEngine) GetTable() string {
	return bse.TableName
}

//插入
func (bse *BigSmartEngine) Insert(data interface{}) (int64, error) {

	//判断是批量还是单个插入
	getValue := reflect.ValueOf(data).Kind()
	if getValue == reflect.Struct {
		return bse.insertData(data, "insert")
	} else if getValue == reflect.Slice || getValue == reflect.Array {
		return bse.batchInsertData(data, "insert")
	} else {
		return 0, errors.New("插入的数据格式不正确，单个插入格式为: struct，批量插入格式为: []struct")
	}
}

//插入数据子方法
func (bse *BigSmartEngine) insertData(data interface{}, insertType string) (int64, error) {

	//反射type和value
	t := reflect.TypeOf(data)
	v := reflect.ValueOf(data)

	//字段名
	var fieldName []string

	//问号?占位符
	var placeholder []string

	//循环判断
	for i := 0; i < t.NumField(); i++ {

		//小写开头，无法反射，跳过
		if !v.Field(i).CanInterface() {
			continue
		}

		//解析tag，找出真实的sql字段名
		sqlTag := t.Field(i).Tag.Get("sql")
		if sqlTag != "" {
			//跳过自增字段
			if strings.Contains(strings.ToLower(sqlTag), "auto_increment") {
				continue
			} else {
				fieldName = append(fieldName, strings.Split(sqlTag, ",")[0])
				placeholder = append(placeholder, "?")
			}
		} else {
			fieldName = append(fieldName, t.Field(i).Name)
			placeholder = append(placeholder, "?")
		}

		//字段值
		bse.AllExec = append(bse.AllExec, v.Field(i).Interface())
	}

	//拼接表，字段名，占位符
	bse.Prepare = insertType + " into " + bse.GetTable() + " (" + strings.Join(fieldName, ",") + ") values(" + strings.Join(placeholder, ",") + ")"

	//prepare
	var stmt *sql.Stmt
	stmt, err := bse.Db.Prepare(bse.Prepare)
	if err != nil {
		return 0, bse.setErrorInfo(err)
	}

	//执行exec,注意这是stmt.Exec
	result, err := stmt.Exec(bse.AllExec...)
	if err != nil {
		return 0, bse.setErrorInfo(err)
	}

	//获取自增ID
	id, _ := result.LastInsertId()
	return id, nil
}

//替换插入
func (bse *BigSmartEngine) Replace(data interface{}) (int64, error) {
	//判断是批量还是单个插入
	getValue := reflect.ValueOf(data).Kind()
	if getValue == reflect.Struct {
		return bse.insertData(data, "replace")
	} else if getValue == reflect.Slice || getValue == reflect.Array {
		return bse.batchInsertData(data, "replace")
	} else {
		return 0, errors.New("插入的数据格式不正确，单个插入格式为: struct，批量插入格式为: []struct")
	}
}

//批量插入
func (bse *BigSmartEngine) batchInsertData(batchData interface{}, insertType string) (int64, error) {

	//反射解析
	getValue := reflect.ValueOf(batchData)

	//切片大小
	l := getValue.Len()

	//字段名
	var fieldName []string

	//占位符
	var placeholderString []string

	//循环判断
	for i := 0; i < l; i++ {
		value := getValue.Index(i) // Value of item
		typed := value.Type()      // Type of item
		if typed.Kind() != reflect.Struct {
			panic("批量插入的子元素必须是结构体类型")
		}

		num := value.NumField()

		//子元素值
		var placeholder []string
		//循环遍历子元素
		for j := 0; j < num; j++ {

			//小写开头，无法反射，跳过
			if !value.Field(j).CanInterface() {
				continue
			}

			//解析tag，找出真实的sql字段名
			sqlTag := typed.Field(j).Tag.Get("sql")
			if sqlTag != "" {
				//跳过自增字段
				if strings.Contains(strings.ToLower(sqlTag), "auto_increment") {
					continue
				} else {
					//字段名只记录第一个的
					if i == 1 {
						fieldName = append(fieldName, strings.Split(sqlTag, ",")[0])
					}
					placeholder = append(placeholder, "?")
				}
			} else {
				//字段名只记录第一个的
				if i == 1 {
					fieldName = append(fieldName, typed.Field(j).Name)
				}
				placeholder = append(placeholder, "?")
			}

			//字段值
			bse.AllExec = append(bse.AllExec, value.Field(j).Interface())
		}

		//子元素拼接成多个()括号后的值
		placeholderString = append(placeholderString, "("+strings.Join(placeholder, ",")+")")
	}

	//拼接表，字段名，占位符
	bse.Prepare = insertType + " into " + bse.GetTable() + " (" + strings.Join(fieldName, ",") + ") values " + strings.Join(placeholderString, ",")

	//prepare
	var stmt *sql.Stmt
	var err error
	stmt, err = bse.Db.Prepare(bse.Prepare)
	if err != nil {
		return 0, bse.setErrorInfo(err)
	}

	//执行exec,注意这是stmt.Exec
	result, err := stmt.Exec(bse.AllExec...)
	if err != nil {
		return 0, bse.setErrorInfo(err)
	}

	//获取自增ID
	id, _ := result.LastInsertId()
	return id, nil
}

//自定义错误格式
func (bse *BigSmartEngine) setErrorInfo(err error) error {
	_, file, line, _ := runtime.Caller(1)
	return errors.New("File: " + file + ":" + strconv.Itoa(line) + ", " + err.Error())
}

//传入and条件
func (bse *BigSmartEngine) Where(data ...interface{}) *BigSmartEngine {

	//判断是结构体还是多个字符串
	var dataType int
	if len(data) == 1 {
		dataType = 1
	} else if len(data) == 2 {
		dataType = 2
	} else if len(data) == 3 {
		dataType = 3
	} else {
		panic("参数个数错误")
	}

	//多次调用判断
	if bse.WhereParam != "" {
		bse.WhereParam += " and ("
	} else {
		bse.WhereParam += "("
	}

	//如果是结构体
	if dataType == 1 {
		t := reflect.TypeOf(data[0])
		v := reflect.ValueOf(data[0])

		//字段名
		var fieldNameArray []string

		//循环解析
		for i := 0; i < t.NumField(); i++ {

			//首字母小写，不可反射
			if !v.Field(i).CanInterface() {
				continue
			}

			//解析tag，找出真实的sql字段名
			sqlTag := t.Field(i).Tag.Get("sql")
			if sqlTag != "" {
				fieldNameArray = append(fieldNameArray, strings.Split(sqlTag, ",")[0]+"=?")
			} else {
				fieldNameArray = append(fieldNameArray, t.Field(i).Name+"=?")
			}

			bse.WhereExec = append(bse.WhereExec, v.Field(i).Interface())
		}

		//拼接
		bse.WhereParam += strings.Join(fieldNameArray, " and ") + ") "

	} else if dataType == 2 {
		//直接=的情况
		bse.WhereParam += data[0].(string) + "=?) "
		bse.WhereExec = append(bse.WhereExec, data[1])
	} else if dataType == 3 {
		//3个参数的情况

		//区分是操作符in的情况
		data2 := strings.Trim(strings.ToLower(data[1].(string)), " ")
		if data2 == "in" || data2 == "not in" {
			//判断传入的是切片
			reType := reflect.TypeOf(data[2]).Kind()
			if reType != reflect.Slice && reType != reflect.Array {
				panic("in/not in 操作传入的数据必须是切片或者数组")
			}

			//反射值
			v := reflect.ValueOf(data[2])
			//数组/切片长度
			dataNum := v.Len()
			//占位符
			ps := make([]string, dataNum)
			for i := 0; i < dataNum; i++ {
				ps[i] = "?"
				bse.WhereExec = append(bse.WhereExec, v.Index(i).Interface())
			}

			//拼接
			bse.WhereParam += data[0].(string) + " " + data2 + " (" + strings.Join(ps, ",") + ")) "

		} else {
			bse.WhereParam += data[0].(string) + " " + data[1].(string) + " ?) "
			bse.WhereExec = append(bse.WhereExec, data[2])
		}
	}

	return bse
}

//传入or条件
func (bse *BigSmartEngine) OrWhere(data ...interface{}) *BigSmartEngine {

	//判断是结构体还是多个字符串
	var dataType int
	if len(data) == 1 {
		dataType = 1
	} else if len(data) == 2 {
		dataType = 2
	} else if len(data) == 3 {
		dataType = 3
	} else {
		panic("参数个数错误")
	}

	if bse.WhereParam == "" {
		panic("WhereOr必须在Where后面调用")
	} else {
		bse.WhereParam += " and ("
	}

	//如果是结构体
	if dataType == 1 {
		t := reflect.TypeOf(data[0])
		v := reflect.ValueOf(data[0])

		//字段名
		var fieldNameArray []string

		//循环解析
		for i := 0; i < t.NumField(); i++ {

			//首字母小写，不可反射
			if !v.Field(i).CanInterface() {
				continue
			}

			//解析tag，找出真实的sql字段名
			sqlTag := t.Field(i).Tag.Get("sql")
			if sqlTag != "" {
				fieldNameArray = append(fieldNameArray, strings.Split(sqlTag, ",")[0]+"=?")
			} else {
				fieldNameArray = append(fieldNameArray, t.Field(i).Name+"=?")
			}

			bse.WhereExec = append(bse.WhereExec, v.Field(i).Interface())
		}

		//拼接
		bse.WhereParam += strings.Join(fieldNameArray, " and ") + ") "

	} else if dataType == 2 {
		//直接=的情况
		bse.WhereParam += data[0].(string) + "=?) "
		bse.WhereExec = append(bse.WhereExec, data[1])
	} else if dataType == 3 {
		//3个参数的情况

		//区分是操作符in的情况
		data2 := strings.Trim(strings.ToLower(data[1].(string)), " ")
		if data2 == "in" || data2 == "not in" {
			//判断传入的是切片
			reType := reflect.TypeOf(data[2]).Kind()
			if reType != reflect.Slice && reType != reflect.Array {
				panic("in/not in 操作传入的数据必须是切片或者数组")
			}

			//反射值
			v := reflect.ValueOf(data[2])
			//数组/切片长度
			dataNum := v.Len()
			//占位符
			ps := make([]string, dataNum)
			for i := 0; i < dataNum; i++ {
				ps[i] = "?"
				bse.WhereExec = append(bse.WhereExec, v.Index(i).Interface())
			}

			//拼接
			bse.WhereParam += data[0].(string) + " " + data2 + " (" + strings.Join(ps, ",") + ")) "

		} else {
			bse.WhereParam += data[0].(string) + " " + data[1].(string) + " ?) "
			bse.WhereExec = append(bse.WhereExec, data[2])
		}
	}

	return bse
}

//删除
func (bse *BigSmartEngine) Delete() (int64, error) {

	//拼接delete sql
	bse.Prepare = "delete from " + bse.GetTable()

	//如果where不为空
	if bse.WhereParam != "" || bse.OrWhereParam != "" {
		bse.Prepare += " where " + bse.WhereParam + bse.OrWhereParam
	}

	//limit不为空
	if bse.LimitParam != "" {
		bse.Prepare += "limit " + bse.LimitParam
	}

	//第一步：Prepare
	var stmt *sql.Stmt
	var err error
	stmt, err = bse.Db.Prepare(bse.Prepare)
	if err != nil {
		return 0, err
	}

	bse.AllExec = bse.WhereExec

	//第二步：执行exec,注意这是stmt.Exec
	result, err := stmt.Exec(bse.AllExec...)
	if err != nil {
		return 0, bse.setErrorInfo(err)
	}

	//影响的行数
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, bse.setErrorInfo(err)
	}

	return rowsAffected, nil
}
