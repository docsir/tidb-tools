package check

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	tc "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

func (t *testCheckSuite) TestTimeCost(c *tc.C) {
	now := time.Now()

	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4", "root", "root", "localhost", 3306)
	db, err := sql.Open("mysql", dbDSN)
	c.Assert(err, tc.IsNil)

	tables := map[string][]string{}
	tables["test1"] = []string{"table1", "table2", "table3"}

	checker := NewTablesChecker(db, &dbutil.DBConfig{}, tables)
	checker.Check(context.Background())

	fmt.Println("Time cost", time.Since(now))
}

func (t *testCheckSuite) TestShardingTablesChecker(c *tc.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, tc.IsNil)
	ctx := context.Background()

	printJson := func(r *Result) {
		rawResult, _ := json.MarshalIndent(r, "", "\t")
		fmt.Println("\n" + string(rawResult))
	}

	// 1. test a success check

	sqlModeRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)

	createTableRow2 := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-2", `CREATE TABLE "test-table-2" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)

	checker := NewShardingTablesChecker("test-name",
		map[string]*sql.DB{"test-source": db},
		map[string]map[string][]string{"test-source": {"test-db": []string{"test-table-1", "test-table-2"}}},
		nil,
		false)
	result := checker.Check(ctx)

	c.Assert(result.State, tc.Equals, StateSuccess)
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)
	printJson(result)

	// 2. check different column number

	sqlModeRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)
	createTableRow2 = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-2", `CREATE TABLE "test-table-2" (
  "c" int(11) NOT NULL,
  "d" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)

	result = checker.Check(ctx)
	c.Assert(result.State, tc.Equals, StateFailure)
	c.Assert(result.Errors, tc.HasLen, 1)
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)
	printJson(result)

	// 3. check different column def

	sqlModeRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)
	createTableRow2 = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-2", `CREATE TABLE "test-table-2" (
  "c" varchar(20) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)

	result = checker.Check(ctx)
	c.Assert(result.State, tc.Equals, StateFailure)
	c.Assert(result.Errors, tc.HasLen, 1)
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)
	printJson(result)
}

func (t *testCheckSuite) TestTablesChecker(c *tc.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, tc.IsNil)
	ctx := context.Background()

	printJson := func(r *Result) {
		rawResult, _ := json.MarshalIndent(r, "", "\t")
		fmt.Println("\n" + string(rawResult))
	}

	// 1. test a success check

	createTableRow := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)
	sqlModeRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)

	checker := NewTablesChecker(db,
		&dbutil.DBConfig{},
		map[string][]string{"test-db": {"test-table-1"}})
	result := checker.Check(ctx)

	c.Assert(result.State, tc.Equals, StateSuccess)
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)
	printJson(result)

	// 2. check many errors

	createTableRow = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL,
  CONSTRAINT "fk" FOREIGN KEY ("c") REFERENCES "t" ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)
	sqlModeRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)

	result = checker.Check(ctx)

	c.Assert(result.State, tc.Equals, StateFailure)
	c.Assert(result.Errors, tc.HasLen, 2) // no PK/UK + has FK
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)
	printJson(result)

	// 3. unsupported charset

	createTableRow = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=gbk`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)
	sqlModeRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)

	result = checker.Check(ctx)

	c.Assert(result.State, tc.Equals, StateFailure)
	c.Assert(result.Errors, tc.HasLen, 1)
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)
	printJson(result)
}
