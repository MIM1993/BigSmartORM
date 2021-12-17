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
	"fmt"
	"reflect"
	"runtime"
	"sync"

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
	mu           sync.Mutex
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
	var err error
	if bse.TransStatus == 1 {
		stmt, err = bse.Tx.Prepare(bse.Prepare)
	} else {
		stmt, err = bse.Db.Prepare(bse.Prepare)
	}
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
	if bse.TransStatus == 1 {
		stmt, err = bse.Tx.Prepare(bse.Prepare)
	} else {
		stmt, err = bse.Db.Prepare(bse.Prepare)
	}
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
	if bse.TransStatus == 1 {
		stmt, err = bse.Tx.Prepare(bse.Prepare)
	} else {
		stmt, err = bse.Db.Prepare(bse.Prepare)
	}
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

func (bse *BigSmartEngine) Update(data ...interface{}) (int64, error) {
	var dataType int
	switch len(data) {
	case 1, 2:
		dataType = len(data)
	default:
		return 0, errors.New("参数个数错误")
	}

	switch dataType {
	case 1:
		t := reflect.TypeOf(data[0])
		v := reflect.ValueOf(data[0])

		var fieldNameArray []string
		for i := 0; i < t.NumField(); i++ {
			if !v.Field(i).CanInterface() {
				continue
			}

			sqlTag := t.Field(i).Tag.Get("sql")
			if sqlTag != "" {
				fieldNameArray = append(fieldNameArray, fmt.Sprintf("%s=?", strings.Split(sqlTag, ",")[0]))
			} else {
				fieldNameArray = append(fieldNameArray, fmt.Sprintf("%s=?", t.Field(i).Name))
			}

			bse.UpdateExec = append(bse.UpdateExec, v.Field(i).Interface())
		}
		bse.UpdateParam += strings.Join(fieldNameArray, ",")

	case 2:
		bse.UpdateParam += data[0].(string) + "=?"
		bse.UpdateExec = append(bse.UpdateExec, data[1])
	}

	bse.Prepare = "update" + bse.GetTable() + " set " + bse.UpdateParam

	if bse.WhereParam != "" || bse.OrWhereParam != "" {
		bse.Prepare += " where " + bse.WhereParam + bse.OrWhereParam
	}

	if bse.LimitParam != "" {
		bse.Prepare += " limit " + bse.LimitParam
	}

	var stmt *sql.Stmt
	var err error
	if bse.TransStatus == 1 {
		stmt, err = bse.Tx.Prepare(bse.Prepare)
	} else {
		stmt, err = bse.Db.Prepare(bse.Prepare)
	}
	if err != nil {
		return 0, bse.setErrorInfo(err)
	}

	if bse.WhereExec != nil {
		bse.AllExec = append(bse.AllExec, bse.WhereExec)
	}

	result, err := stmt.Exec(bse.AllExec)
	if err != nil {
		return 0, bse.setErrorInfo(err)
	}

	id, _ := result.RowsAffected()
	return id, nil
}

func (bse *BigSmartEngine) Select() ([]map[string]string, error) {

	bse.Prepare += "select * from " + bse.GetTable()

	if bse.WhereParam != "" || bse.OrWhereParam != "" {
		bse.Prepare += " where " + bse.WhereParam + bse.OrWhereParam
	}

	//group 不为空
	if bse.GroupParam != "" {
		bse.Prepare += " group by " + bse.GroupParam
	}

	//having
	if bse.HavingParam != "" {
		bse.Prepare += " having " + bse.HavingParam
	}

	bse.AllExec = append(bse.AllExec, bse.WhereExec)

	var err error
	var rows *sql.Rows
	if bse.TransStatus == 1 {
		rows, err = bse.Tx.Query(bse.Prepare, bse.AllExec)
	} else {
		rows, err = bse.Db.Query(bse.Prepare, bse.AllExec)
	}
	if err != nil {
		return nil, bse.setErrorInfo(err)
	}

	colume, err := rows.Columns()
	if err != nil {
		return nil, bse.setErrorInfo(err)
	}

	values := make([][]byte, len(colume))
	scans := make([]interface{}, len(colume))

	for i := range values {
		scans[i] = &values[i]
	}

	result := make([]map[string]string, 0)
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return nil, bse.setErrorInfo(err)
		}

		//row
		row := make(map[string]string)
		for k, v := range values {
			row[colume[k]] = string(v)
		}
		result = append(result, row)
	}

	return result, nil
}

func (bse *BigSmartEngine) SelectOne() (map[string]string, error) {
	//limit 1 单个查询
	results, err := bse.Limit(1).Select()
	if err != nil {
		return nil, bse.setErrorInfo(err)
	}

	//判断是否为空
	if len(results) == 0 {
		return nil, nil
	} else {
		return results[0], nil
	}
}

func (bse *BigSmartEngine) Find(data interface{}) error {
	v := reflect.ValueOf(data)

	if v.Kind() != reflect.Ptr {
		return bse.setErrorInfo(errors.New("参数请传指针变量!"))
	}

	if v.IsNil() {
		return bse.setErrorInfo(errors.New("参数不能是空指针!"))
	}

	bse.Prepare = "SELECT * FROM " + bse.GetTable()

	bse.AllExec = bse.WhereExec

	//group 不为空
	if bse.GroupParam != "" {
		bse.Prepare += " group by " + bse.GroupParam
	}

	//having
	if bse.HavingParam != "" {
		bse.Prepare += " having " + bse.HavingParam
	}

	var err error
	var rows *sql.Rows
	if bse.TransStatus == 1 {
		rows, err = bse.Tx.Query(bse.Prepare, bse.AllExec)
	} else {
		rows, err = bse.Db.Query(bse.Prepare, bse.AllExec)
	}
	if err != nil {
		return bse.setErrorInfo(err)
	}

	column, err := rows.Columns()
	if err != nil {
		return bse.setErrorInfo(err)
	}

	values := make([][]byte, len(column))
	scans := make([]interface{}, len(column))

	destSlice := v.Elem()

	destType := v.Type().Elem()

	for i := range scans {
		scans[i] = &values[i]
	}

	for rows.Next() {
		dest := reflect.New(destType).Elem()

		if err := rows.Scan(scans...); err != nil {
			return bse.setErrorInfo(err)
		}

		for k, v := range values {
			key := column[k]
			value := string(v)

			//遍历结构体
			for i := 0; i < destType.NumField(); i++ {
				sqlTag := destType.Field(i).Tag.Get("sql")
				var filedName string
				if sqlTag != "" {
					filedName = strings.Split(sqlTag, ",")[0]
				} else {
					filedName = destType.Field(i).Name
				}
				if key != filedName {
					continue
				}

				if err := bse.reflectSet(dest, i, value); err != nil {
					return err
				}
			}

		}
		destSlice.Set(reflect.AppendSlice(destSlice, dest))
	}
	return nil
}

//反射赋值
func (e *BigSmartEngine) reflectSet(dest reflect.Value, i int, value string) error {
	switch dest.Field(i).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		res, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return e.setErrorInfo(err)
		}
		dest.Field(i).SetInt(res)
	case reflect.String:
		dest.Field(i).SetString(value)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		res, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return e.setErrorInfo(err)
		}
		dest.Field(i).SetUint(res)
	case reflect.Float32:
		res, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return e.setErrorInfo(err)
		}
		dest.Field(i).SetFloat(res)
	case reflect.Float64:
		res, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return e.setErrorInfo(err)
		}
		dest.Field(i).SetFloat(res)
	case reflect.Bool:
		res, err := strconv.ParseBool(value)
		if err != nil {
			return e.setErrorInfo(err)
		}
		dest.Field(i).SetBool(res)
	}
	return nil
}

func (bse *BigSmartEngine) FindOne(data interface{}) error {
	dest := reflect.Indirect(reflect.ValueOf(data))

	//生成数据容器
	destSlice := reflect.New(reflect.SliceOf(dest.Type())).Elem()

	if err := bse.Limit(1).Find(destSlice.Addr().Interface()); err != nil {
		return err
	}

	if destSlice.Len() == 0 {
		return bse.setErrorInfo(errors.New("NOT FOUND"))
	}

	dest.Set(destSlice.Index(0))
	return nil
}

//limit分页
func (bse *BigSmartEngine) Limit(limit ...int64) *BigSmartEngine {
	if len(limit) == 1 {
		bse.LimitParam = strconv.Itoa(int(limit[0]))
	} else if len(limit) == 2 {
		bse.LimitParam = strconv.Itoa(int(limit[0])) + "," + strconv.Itoa(int(limit[1]))
	} else {
		panic("参数个数错误")
	}
	return bse
}

//设置查询字段Field
func (bse *BigSmartEngine) Field(field ...string) *BigSmartEngine {
	bse.FieldParam = strings.Join(field, ",")

	return bse
}

func (bse *BigSmartEngine) aggregateQuery(name, param string) (interface{}, error) {
	bse.Prepare = " select " + name + "(" + param + ") as cnt from" + bse.GetTable()

	if bse.WhereParam != "" || bse.OrWhereParam != "" {
		bse.Prepare += bse.WhereParam + bse.OrWhereParam
	}

	if bse.LimitParam != "" {
		bse.Prepare += " limit " + bse.LimitParam
	}

	bse.AllExec = bse.WhereExec

	bse.generateSql()

	var cnt interface{}

	if err := bse.Db.QueryRow(bse.Prepare, bse.AllExec).Scan(cnt); err != nil {
		return nil, err
	}
	return cnt, nil
}

func (bse *BigSmartEngine) generateSql() {
	bse.Sql = bse.Prepare
	for _, v := range bse.AllExec {
		switch v.(type) {
		case int:
			bse.Sql = strings.Replace(bse.Sql, "?", strconv.Itoa(v.(int)), 1)
		case int64:
			bse.Sql = strings.Replace(bse.Sql, "?", strconv.FormatInt(v.(int64), 10), 1)
		case bool:
			bse.Sql = strings.Replace(bse.Sql, "?", strconv.FormatBool(v.(bool)), 1)
		default:
			bse.Sql = strings.Replace(bse.Sql, "?", fmt.Sprintf(" '%s'", v.(string)), 1)
		}
	}
}

func (bse *BigSmartEngine) Max(param string) (string, error) {
	max, err := bse.aggregateQuery("max", param)
	if err != nil {
		return "0", err
	}
	return string(max.([]byte)), nil
}

func (bse *BigSmartEngine) Min(param string) (string, error) {
	min, err := bse.aggregateQuery("min", param)
	if err != nil {
		return "0", err
	}
	return string(min.([]byte)), nil
}

func (bse *BigSmartEngine) Avg(param string) (string, error) {
	avg, err := bse.aggregateQuery("avg", param)
	if err != nil {
		return "0", err
	}
	return string(avg.([]byte)), nil
}

func (bse *BigSmartEngine) Sum(param string) (string, error) {
	sum, err := bse.aggregateQuery("sum", param)
	if err != nil {
		return "0", err
	}
	return string(sum.([]byte)), nil
}

func (bse *BigSmartEngine) Count(param string) (string, error) {
	count, err := bse.aggregateQuery("count", param)
	if err != nil {
		return "0", err
	}
	return string(count.([]byte)), nil
}

func (bse *BigSmartEngine) Order(data ...string) *BigSmartEngine {
	if len(data)%2 != 0 {
		panic("order by参数错误，请保证个数为偶数个")
	}

	orderNum := len(data) / 2

	if bse.OrderParam != "" {
		bse.OrderParam += ","
	}

	for i := 0; i < orderNum; i++ {
		key := strings.ToLower(data[i*2+1])
		if key != "desc" && key != "asc" {
			panic("排序关键字为：desc和asc")
		}
		if i < orderNum-1 {
			bse.OrderParam += fmt.Sprintf("%s %s,", data[i*2], data[i*2+1])
		} else {
			bse.OrderParam += fmt.Sprintf("%s %s", data[i*2], data[i*2+1])
		}
	}

	return bse
}

func (bse *BigSmartEngine) Group(group ...string) *BigSmartEngine {
	if len(group) != 0 {
		bse.GroupParam = strings.Join(group, ",")
	}
	return bse
}

func (bse *BigSmartEngine) Having(having ...interface{}) *BigSmartEngine {
	var dataType int
	switch len(having) {
	case 1, 2, 3:
		dataType = len(having)
	default:
		panic("having个数错误")
	}

	if bse.HavingParam != "" {
		bse.HavingParam += "and ("
	} else {
		bse.HavingParam += "("
	}

	switch dataType {
	case 1: //结构体
		t := reflect.TypeOf(having[0])
		v := reflect.ValueOf(having[0])

		var fieldName []string
		for i := 0; i < t.NumField(); i++ {
			if !v.Field(i).CanInterface() {
				continue
			}

			sqlTag := t.Field(i).Tag.Get("sql")
			if sqlTag != "" {
				fieldName = append(fieldName, strings.Split(sqlTag, ",")[0]+"=?")
			} else {
				fieldName = append(fieldName, t.Field(i).Name+"=?")
			}
			bse.WhereExec = append(bse.WhereExec, v.Field(i).Interface())
		}
		bse.HavingParam += strings.Join(fieldName, "and") + ")"
	case 2: // =
		bse.HavingParam += having[0].(string) + "=?)"
		bse.WhereExec = append(bse.WhereExec, having[1])
	case 3: // > >= < <= !=
		bse.HavingParam += having[0].(string) + " " + having[1].(string) + "?)"
		bse.WhereExec = append(bse.WhereExec, having[2])
	}

	return bse
}

func (bse *BigSmartEngine) Exec(sql string) (int64, error) {
	result, err := bse.Db.Exec(sql)
	if err != nil {
		return 0, bse.setErrorInfo(err)
	}

	bse.Sql = sql

	//insert
	if strings.Contains(sql, "insert") {
		lastInsertId, _ := result.LastInsertId()
		return lastInsertId, nil
	}
	rows, _ := result.RowsAffected()
	return rows, nil
}

func (bse *BigSmartEngine) Query(sql string) ([]map[string]string, error) {
	result, err := bse.Db.Query(sql)
	if err != nil {
		return nil, bse.setErrorInfo(err)
	}

	bse.Sql = sql

	column, err := result.Columns()
	if err != nil {
		return nil, bse.setErrorInfo(err)
	}

	values := make([][]byte, len(column))
	scans := make([]interface{}, len(column))
	for i := range values {
		scans[i] = &values[i]
	}

	resultMap := make([]map[string]string, 0)
	for result.Next() {
		if err := result.Scan(scans...); err != nil {
			return nil, bse.setErrorInfo(err)
		}
		row := make(map[string]string, len(column))
		for k, v := range values {
			row[column[k]] = string(v)
		}
		resultMap = append(resultMap, row)
	}
	return resultMap, nil
}

func (bse *BigSmartEngine) Begin() error {
	if bse.TransStatus == 1 || bse.Tx != nil {
		return errors.New("Transaction already exists")
	}
	bse.mu.Lock()
	tx, err := bse.Db.Begin()
	if err != nil {
		return err
	}
	bse.TransStatus = 1
	bse.Tx = tx
	bse.mu.Unlock()
	return nil
}

func (bse *BigSmartEngine) Rollback() error {
	if bse.TransStatus != 1 || bse.Tx == nil {
		return errors.New("Transaction not exists")
	}
	bse.mu.Lock()
	err := bse.Tx.Rollback()
	if err != nil {
		return err
	}
	bse.TransStatus = 0
	bse.mu.Unlock()
	return nil
}

func (bse *BigSmartEngine) Commit() error {
	if bse.TransStatus != 1 || bse.Tx == nil {
		return errors.New("Transaction not exists")
	}
	bse.mu.Lock()
	err := bse.Tx.Commit()
	if err != nil {
		return err
	}
	bse.TransStatus = 0
	bse.Tx = nil
	bse.mu.Unlock()
	return nil
}
