/*
@Time : 2021/12/5 下午2:36
@Author : MuYiMing
@File : mysqlConn
@Software: GoLand
*/
package BigSmartORM

import "database/sql"

func NewMysqlConn(Username string, Password string, Address string, Dbname string) (*BigSmartEngine, error){
	dsn := Username + ":" + Password + "@tcp(" + Address + ")/" + Dbname + "?charset=utf8&timeout=5s&readTimeout=6s"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	//最大连接数等配置，先占个位
	//db.SetMaxOpenConns(3)
	//db.SetMaxIdleConns(3)

	return &BigSmartEngine{
		Db:         db,
		FieldParam: "*",
	}, nil
}
