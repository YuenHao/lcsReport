package com.pingan.lcs.report.streaming

import org.junit._
import Assert._
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.examples.streaming.StaffInfo

@Test
class ReportRead2MongoStreaming {

  val updateJson =
    """
      |{"data":[{"deptno":"112013601","empno":"1234567890","emp_name":"周财211","sex":"M","birth_date":"1970-01-01","id_type":"1","emp_idno":"440103197007013070","hire_date":"2008-01-01","reg_date":"2008-05-01","leave_date":null,"fcd":"2008-01-01","fcu":"李工","pk_serial":"2"}],"database":"report","es":1543909620000,"id":5,"isDdl":false,"mysqlType":{"deptno":"varchar(11)","empno":"varchar(10)","emp_name":"varchar(50)","sex":"varchar(1)","birth_date":"date","id_type":"varchar(1)","emp_idno":"varchar(18)","hire_date":"date","reg_date":"date","leave_date":"date","fcd":"date","fcu":"varchar(30)","pk_serial":"int(32)"},"old":[{"emp_name":"周财2"}],"sql":"","sqlType":{"deptno":12,"empno":12,"emp_name":12,"sex":12,"birth_date":91,"id_type":12,"emp_idno":12,"hire_date":91,"reg_date":91,"leave_date":91,"fcd":91,"fcu":12,"pk_serial":4},"table":"staff_info","ts":1543909620416,"type":"UPDATE"}
    """.stripMargin
  @Test
  def testFastJson2StaffInfo() = {

    val updateObj: JSONObject = JSON.parseObject(updateJson)

    assertEquals(updateObj.getString("type"), "UPDATE")

    val dataStr: String = updateObj.getString("data")

    val dataObj = JSON.parseObject(dataStr.substring(1, dataStr.size - 1), classOf[StaffInfo])

    assertTrue(dataObj.isInstanceOf[StaffInfo])

  }


  //    @Test
  //    def testKO() = assertTrue(false)

}