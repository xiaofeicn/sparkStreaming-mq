package com.cj.spark.streaming.streaming


import java.util.Properties

import com.alibaba.fastjson.JSON
import com.cj.spark.streaming.models.{tb_question_msg, tb_stu_msg, tb_teach_msg}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.spark_project.jetty.server.{Request, Server}
import org.spark_project.jetty.server.handler.{AbstractHandler, ContextHandler}
import util.ConfigerHelper
import com.cj.spark.streaming.models.RabbitMqConfig.{rabbitMqMaps, rabbitMqMapsTest}
import org.apache.log4j.Logger

object Streaming extends Serializable {
  private[this] val defaultFS = ConfigerHelper.getProperty("defaultFS")
  private[this] val checkpointDirectory = ConfigerHelper.getProperty("checkpointDirectory")
  private[this] val jdbcUrlTest = ConfigerHelper.getProperty("jdbc.url.test")
  private[this] val jdbcUserTest = ConfigerHelper.getProperty("jdbc.user.test")
  private[this] val jdbcPasswordTest = ConfigerHelper.getProperty("jdbc.password.test")
  private[this] val appName = ConfigerHelper.getProperty("appName")
  private[this] val thisCheckpointDirectory = checkpointDirectory + appName
  val log: Logger = org.apache.log4j.LogManager.getLogger(appName)


  def main(args: Array[String]): Unit = {
    val sct = StreamingContext.getOrCreate(thisCheckpointDirectory,
      () => {
        createStreamingContext(thisCheckpointDirectory, appName)
      })
    sct.start()
    daemonHttpServer(5555, sct)
    sct.awaitTermination()
  }


  def createStreamingContext(checkpointDirectory: String, appName: String): StreamingContext = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    sc.setCheckpointDir(checkpointDirectory)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(30))
    //    ssc.checkpoint(checkpointDirectory)
    val ds = RabbitMQUtils.createStream(ssc, rabbitMqMapsTest)

    //    val p = ds.transform(rdd => rdd.map(x => defaultFS + x))

    ds.foreachRDD(rdd => {

      val p = rdd.map(x => defaultFS + x).collect()

      for (filePath <- p) {
        //        println(filePath)
        val jsonRDD = sc.textFile(filePath).map(x => JSON.parseObject(x))
        val jsonArrayRDD = jsonRDD.map(x => x.getJSONArray("RecordList").toArray().map(x => JSON.parseObject(x.toString)))

        val json_name_dataARR = jsonArrayRDD.flatMap(x =>
          x.map(s => (s.getString("fStr_TableName"), s.getJSONArray("RecordList").toArray()))
        ).persist(StorageLevel.MEMORY_ONLY)
        json_name_dataARR.checkpoint()

        //        json_name_dataARR.foreach(x => {
        //        //          println(x._1)
        //        //        })
        //        //        json_name_dataARR.filter(x => x._1.contains("tb_tech_exam")).foreach(f => {
        //        //          println(f._1)
        //        //          f._2.foreach(k => {
        //        //            print(k)
        //        //            print(",")
        //        //          })
        //        //          println()
        //        //        })

        val stuLogin = json_name_dataARR.filter(_._1 == "tb_tech_student_login_record").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getString("fStr_StudentID"),
          json.getString("fStr_StudentName"),
          json.getString("fStr_CourseID")
        )).distinct()
        //取出课堂ID
        val courseID = stuLogin.map(_._3).first()

        val stuLoginRdd = stuLogin.map(data => (data._1, data._2))
        /**
          * 上课签到人数
          */
        val loginCount = stuLoginRdd.count().toInt


        /**
          * 双向互动
          */
        val interactRdd = json_name_dataARR.filter(_._1 == "tb_tech_interact").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType")
        )).filter(x => x._3 == 102 || x._3 == 103 || x._3 == 111)
        val groupInteractRdd = json_name_dataARR.filter(_._1 == "tb_tech_group_interact").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType")
        )).filter(x => x._3 == 402 || x._3 == 411)
        val investigationRdd = json_name_dataARR.filter(_._1 == "tb_tech_investigation").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType")
        )).filter(x => x._3 == 201 || x._3 == 202 || x._3 == 203 || x._3 == 205)
        val practiseRdd = json_name_dataARR.filter(_._1 == "tb_tech_practise").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType")
        )).filter(x => x._3 == 301 || x._3 == 302 || x._3 == 303 || x._3 == 304 || x._3 == 305)

        val beforeClassInvestigRdd = json_name_dataARR.filter(_._1 == "tb_tech_before_class_investig").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType")
        )).filter(x => x._3 == 202 || x._3 == 203)

        val classExamCount = json_name_dataARR.filter(_._1 == " tb_tech_exam").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => json.getString("fStr_ExamID")).distinct().count().toInt

        val interact_issue = interactRdd.union(groupInteractRdd).union(investigationRdd).union(practiseRdd).union(beforeClassInvestigRdd)
          .cache()
        //双向互动发布次数
        val interact_count = interact_issue.count().toInt + classExamCount

        //每个双向互动应影响人数
        val interactStuTotal = interact_issue.map(data => (data._3, loginCount)).distinct()
        //        interactStuTotal.foreach(println(_))

        /**
          * 互动响应
          */

        val interactReplyRdd = json_name_dataARR.filter(_._1 == "tb_tech_interact_reply").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fStr_StudentID"),
          json.getString("fDtt_AnswerTime")
        )).filter(x => x._3 == 102 || x._3 == 103 || x._3 == 111)
          .distinct()

        val examReplyRdd = json_name_dataARR.filter(_._1 == "tb_tech_exam_reply").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fStr_StudentID"),
          json.getString("fDtt_AnswerTime")
        )).filter(x => x._3 == 601 || x._3 == 602 || x._3 == 603 || x._3 == 604 || x._3 == 605 || x._3 == 606 || x._3 == 607)
          .map(data => (data._1, data._2, 600, data._4, data._5))

        val groupInteractReplyRdd = json_name_dataARR.filter(_._1 == "tb_tech_group_interact_reply").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fStr_StudentID"),
          json.getString("fDtt_AnswerTime")
        )).filter(x => x._3 == 402 || x._3 == 411)
          .distinct()
        val investigationReplyRdd = json_name_dataARR.filter(_._1 == "tb_tech_investigation_reply").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fStr_StudentID"),
          json.getString("fDtt_AnswerTime")
        )).filter(x => x._3 == 201 || x._3 == 202 || x._3 == 203 || x._3 == 205)
          .distinct()

        val beforeClassInvestigRddReplyRdd = json_name_dataARR.filter(_._1 == "tb_tech_before_class_investig_reply").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fStr_StudentID"),
          json.getString("fDtt_AnswerTime")
        ))
          .distinct()

        val practiseReplyRdd = json_name_dataARR.filter(_._1 == "tb_tech_practise_reply").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fStr_StudentID"),
          json.getString("fDtt_AnswerTime")
        )).filter(x => x._3 == 301 || x._3 == 302 || x._3 == 303 || x._3 == 304 || x._3 == 305)
          .distinct()

        val evaluateReplyRdd = json_name_dataARR.filter(_._1 == "tb_tech_evaluate").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          111,
          json.getString("fStr_StudentID"),
          json.getString("fDtt_CreateTime")
        )).distinct()

        val groupEvaluateReplyRdd = json_name_dataARR.filter(_._1 == "tb_tech_group_evaluate").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          411,
          json.getString("fStr_StudentID"),
          json.getString("fDtt_CreateTime")
        )).distinct()

        val contributionReplyRdd = json_name_dataARR.filter(_._1 == "tb_tech_group_contribution").flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          411,
          json.getString("fStr_StudentID"),
          json.getString("fDtt_CreateTime")
        )).distinct()

        /**
          * type_stu_times 互动类型编号 学生ID 此学生次互动次数
          */

        val type_stu_times = groupInteractReplyRdd //所有类型互动响应，根据每个学生每个响应最迟时间
          .union(groupInteractReplyRdd)
          .union(investigationReplyRdd)
          .union(practiseReplyRdd)
          .union(groupEvaluateReplyRdd)
          .union(evaluateReplyRdd)
          .union(contributionReplyRdd)
          .union(beforeClassInvestigRddReplyRdd)
          .union(interactReplyRdd)
          .union(examReplyRdd)
          .distinct().coalesce(5)
          .map(data => ((data._1, data._2, data._3, data._4), data._5)).groupByKey().
          mapValues(f => {
            f.toList.sorted.reverse.head
          }).map(data => (data._1._1, data._1._2, data._1._3, data._1._4, data._2))
          .map(type_num => ((type_num._3, type_num._4), 1))
          .reduceByKey(_ + _).cache()


        /**
          * response  互动类型编号  学生个数
          */
        val response = type_stu_times.
          map(data => (data._1._1, 1))
          .reduceByKey(_ + _).cache()

        /**
          * noResponseManCount  此互动 (互动人数,签到人数)
          */
        val noResponseManCount = response.cogroup(interactStuTotal)
          .map(data => (data._1, diffValue(data._2._1, data._2._2)))
          .filter(_._2 > 0)
          .sortBy(_._2, false).collect()

        /**
          * allInteractManCount  所有互动人数SUM
          */
        val allInteractManCount = if (response.isEmpty()) 0 else response
          .map(data => data._2).reduce(_ + _).toDouble
        //        println(s"各互动未响应人数")

        val noResponseManCountMsg = if (noResponseManCount.isEmpty) (-1, -1) else noResponseManCount(0)

        //        println(s"所有互动的总人数$allInteractManCount")

        /**
          * attention 所有互动人数/（签到人数*发布的互动数）
          */
        val focusRate = (allInteractManCount / (loginCount * interact_count.toDouble) * 100).round
        //        println(s"1.3专注度:$focusRate %")
        //        println(loginCount * interact_count)

        /**
          * practiseQuestionRdd 练习题发布
          */
        val practiseQuestionRdd = json_name_dataARR.filter(_._1 == "tb_tech_question")
          .flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_QuestionType"),
          json.getString("fDtt_CreateTime"),
          json.getIntValue("fInt_QuestionNo"),
          json.getIntValue("fInt_SubQuesNo"),
          json.getString("fStr_RightAnswerText")
        )).filter(x => x._3 == 301 || x._3 == 302 || x._3 == 303 || x._3 == 304 || x._3 == 305)
          .distinct().cache()
        /**
          * practiseAnswerRdd 练习题回答
          */
        val practiseAnswerRdd = json_name_dataARR.filter(_._1 == "tb_tech_practise_reply")
          .flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fDtt_AnswerTime"),
          json.getIntValue("fInt_QuestionNo"),
          json.getIntValue("fInt_SubQuesNo"),
          json.getString("fStr_AnswerText"),
          json.getIntValue("fInt_SubmitType"),
          json.getString("fStr_StudentID")
        )).filter(x => x._3 == 301 || x._3 == 302 || x._3 == 303 || x._3 == 304 || x._3 == 305)
          .distinct().filter(_._8 == 1).cache()

        val keyQA = practiseQuestionRdd.map(data => ((data._1, data._2, data._5, data._6, data._4), (data._3, data._7)))
        val key = practiseQuestionRdd.map(data => (
          (data._1, data._2, data._5, data._6), data._4)
        ).groupByKey()
          .mapValues(f => {
            f.toList.sorted.reverse.head
          }).map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._2), 1))
        /**
          * 练习题问题及答案
          */
        val questionAnswer = key.join(keyQA).map(data => (
          (data._1._1, data._1._2, data._1._3, data._1._4), (data._2._2._1, data._2._2._2)
        )).cache()

        /**
          * 客观题问题及答案
          */
        val objectivesQuestionAnswer = questionAnswer.filter(
          x => x._2._1 == 301 ||
            x._2._1 == 302 ||
            x._2._1 == 303 ||
            x._2._1 == 304)
          .map(data => (
            (data._1._1, data._1._2, data._1._3, data._1._4, data._2._1), data._2._2)
          )

        /**
          * 主观题问题及答案
          */
        val subjectiveQuestionAnswer = questionAnswer.filter(_._2._1 == 305).map(data => (
          (data._1._1, data._1._2, data._1._3, data._1._4, data._2._1), data._2._2)
        )

        val questionAnswerMap = questionAnswer.map(data => (
          (data._1._1, data._1._2, data._1._3, data._1._4, data._2._1), data._2._2)
        ).collectAsMap()


        val PAKey = practiseAnswerRdd.map(data => (
          (data._1, data._2, data._3, data._5, data._6, data._9), data._4)
        ).groupByKey()
          .mapValues(f => {
            f.toList.sorted.reverse.head
          }).map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._1._5, data._1._6, data._2), 1))
        val PAValue = practiseAnswerRdd.map(data => (
          (data._1, data._2, data._3, data._5, data._6, data._9, data._4), data._7)
        )
        /**
          * 每个学生每道题最后所答
          */
        val practiseLastAnswer = PAKey.join(PAValue).map(data => ((
          data._1._1, data._1._2, data._1._4, data._1._5, data._1._3), (data._1._6, data._2._2))
        ).cache()
        /**
          * 客观题正确率
          */
        val objectivesLastAnswer = practiseLastAnswer
          .filter(x => x._1._5 == 301 || x._1._5 == 302 || x._1._5 == 303 || x._1._5 == 304)
          .filter(x => questionAnswerMap.keySet.contains(x._1))
        val objectivesRight = objectivesLastAnswer.filter(x => show(questionAnswerMap.get(x._1)) == x._2._2)
        val objectivesRightNum = objectivesRight.count().toDouble
        val objectivesQuestionAnswerNum = objectivesQuestionAnswer.count()
        //  println(s"客观正确数$objectivesRightNum")
        //  println(s"客观发布数$objectivesQuestionAnswerNum")
        val objectivesRightRatio = (objectivesRightNum / (objectivesQuestionAnswerNum * loginCount.toDouble) * 100).round

        //        println(s"2.2客观题正确率：$objectivesRightRatio %")

        /**
          * 主观题完成率
          */
        val subjectiveLastAnswer = practiseLastAnswer.filter(_._1._5 == 305)
          .filter(x => questionAnswerMap.keySet.contains(x._1))
        val subjectiveLastAnswerNum = subjectiveQuestionAnswer.count()
        val subjectiveOverRatio = (subjectiveLastAnswer.count().toDouble /
          (subjectiveLastAnswerNum * loginCount.toDouble) * 100)
          .round
        //        println(s"2.1主观题完成率 $subjectiveOverRatio %")

        /**
          * 主观+客观    正确率
          * （客观题正确数*10+主观题获得分）/客观题发布数*签到人数*10+有效[批改并且有分主动提交]
          */


        val practiseCorrectRdd = json_name_dataARR.filter(_._1 == "tb_tech_practise_correct")
          .flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_SubmitType"),
          json.getString("fDtt_CreateTime"),
          json.getIntValue("fInt_QuestionNo"),
          json.getIntValue("fInt_SubQuesNo"),
          json.getString("fStr_CorrectID"),
          json.getDoubleValue("fFlt_Score")
        )).filter(x => x._3 == 1 && x._8 != -1)
          .distinct().cache()
        val practiseCorrectLastTime = practiseCorrectRdd
          .map(data => ((data._1, data._2, data._5, data._6, data._7), data._4))
          .groupByKey().mapValues(f => {
          f.toList.sorted.reverse.head
        }).map(data => (
          (data._1._1, data._1._2, data._1._3, data._1._4, data._1._5, data._2), 1)
        )
        val practiseCorrectLast = practiseCorrectRdd
          .map(data => ((data._1, data._2, data._5, data._6, data._7, data._4), data._8))
          .join(practiseCorrectLastTime)
          .map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._1._5), data._2._1))


        val correctRate = ((if (objectivesRight.isEmpty()) 0 else objectivesRight.count() * 10 +
          practiseCorrectLast.map(_._2).reduce(_ + _)) /
          (objectivesQuestionAnswer.count() * loginCount * 10 + practiseCorrectLast.count() * 10) * 100).round
        //        println(s"2.3全部正确率：$correctRate % ")


        /**
          * 预习率&复习率
          */
        val investigationClassRdd = json_name_dataARR.filter(_._1 == "tb_tech_before_class_investig_reply")
          .flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fStr_StudentID"),
          json.getString("fStr_AnswerText"),
          json.getString("fDtt_AnswerTime")
        )).filter(x => x._3 == 202 || x._3 == 203)
          .distinct().cache()

        val investigationClassLastTmp = investigationClassRdd.map(data => (
          (data._1, data._2, data._3, data._4), data._6)
        ).groupByKey()
          .mapValues(f => {
            f.toList.sorted.reverse.head
          }).map(data => (
          (data._1._1, data._1._2, data._1._3, data._1._4, data._2), 1)
        )

        val investigationClassLast = investigationClassRdd.map(data => (
          (data._1, data._2, data._3, data._4, data._6), data._5)
        ).join(investigationClassLastTmp)
        val beforeClass = investigationClassLast.filter(_._1._3 == 202) //预习数据
        val beforeClassInvestigationManCount_yes = beforeClass.filter(_._2._1 == "预习了").count()
        //预习人数
        val beforeClassInvestigationManCount = beforeClass.count()
        //预习调查人数
        val beforeClassRatio = (beforeClassInvestigationManCount_yes.toDouble /
          beforeClassInvestigationManCount.toDouble * 100).round
        //        println(s"预习率：$beforeClassRatio%")


        val afterClass = investigationClassLast.filter(_._1._3 == 203) //复习数据
        val afterClassInvestigationManCount_yes = afterClass.filter(_._2._1 == "复习了").count()
        //复习人数
        val afterClassInvestigationManCount = afterClass.count()
        //复习调查人数
        val afterClassRatio = (afterClassInvestigationManCount_yes.toDouble /
          afterClassInvestigationManCount.toDouble * 100).round
        //        println(s"复习率：$afterClassRatio%")


        /**
          * 个人关注度
          */
        //        println("个人专注度")
        val stu_times = type_stu_times.map(data => (data._1._2, data._2)).reduceByKey(_ + _)
        val everyAttention = stu_times.map(data => (data._1, (data._2.toDouble /
          interact_count.toDouble * 100).round))
        //        everyAttention.foreach(println(_))

        /**
          * 个人客观题回答正确率
          */
        //每个学会回答客观题正确题数
        val everyObjectivesRight = objectivesRight.map(data =>
          (data._2._1, 1)
        ).reduceByKey(_ + _)

        val everyBodyObjectivesRightRatio = everyObjectivesRight.map(data =>
          (data._1, (data._2.toDouble / objectivesQuestionAnswerNum * 100).round)
        )
        //        println(s"个人客观题回答正确率 ${everyBodyObjectivesRightRatio.count()}")
        //        everyBodyObjectivesRightRatio.foreach(println(_))

        /**
          * 客观题每道正确率
          *
          */
        val everyObjectivesRightCount = objectivesQuestionAnswer
          .join(objectivesRight.map(data => (data._1, 1)).reduceByKey(_ + _))
          .map(data => (data._1, data._2._2))
        //        println("客观题每道正确率 ")
        val everyObjectivesRightRatio = everyObjectivesRightCount.map(data =>
          (data._1, (data._2.toDouble / loginCount.toDouble * 100).round))
        //        everyObjectivesRightRatio.foreach(println(_))

        /**
          * 主观题每道题完成率
          */

        val everySubjectiveOverCount = subjectiveQuestionAnswer
          .join(subjectiveLastAnswer.map(data => (data._1, 1))
            .reduceByKey(_ + _))
          .map(data => (data._1, data._2._2))
        val everySubjectiveOverRatio = everySubjectiveOverCount.map(data =>
          (data._1, (data._2.toDouble / loginCount.toDouble * 100).round))
        //        println("主观题每道题完成率")
        //        everySubjectiveOverRatio.foreach(println(_))
        /**
          * 选择&判断每道题选择人数，未答人
          */

        val choice = objectivesLastAnswer.filter(x => x._1._5 == 301 || x._1._5 == 302 || x._1._5 == 304).cache()
        val choiceAnswerNum = choice
          .map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._1._5, data._2._2), data._2._1))
          .groupByKey().map(data => (data._1, data._2.size))
        //每道选择题的每个选项选择人数
        val loginMap = stuLoginRdd.collectAsMap()
        //        println("每个选项的选择人数")
        //        choiceAnswerNum.foreach(println(_))

        val everyChoiceAnswerNum = choiceAnswerNum.map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._1._5), (data._1._6, data._2)))
          .groupByKey().mapValues(f => {
          f.map(data => data._1 + ":" + data._2).mkString(",")
        })


        //        println("每道题的未答人")
        val everyQuseionNoAnswer = choice.map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._1._5), data._2._1)).groupByKey()
          .map(data => (data._1, {
            loginMap.keySet -- data._2.toSet
          })).filter(_._2.nonEmpty).map(data => (data._1, data._2.mkString(",")))

        /**
          * 对应题
          */
        val corresponding = objectivesLastAnswer.filter(x => x._1._5 == 303)
        /*对应题每一道题全对人 对等对应题每道题错几道选项中的【0：人数】*/
        val everyCorrespondingRightCount = objectivesRight
          .filter(x => x._1._5 == 303).map(data => (data._1, data._2._1)).
          groupByKey()
          .map(data => (data._1, data._2.size))
        //        everyCorrespondingRightCount.foreach(println(_))
        val correspondingTmp = corresponding.map(data => (data._1, data._2._2))
        val everyCorrespondingMistakeNumRdd = objectivesQuestionAnswer
          .filter(x => x._1._5 == 303).join(correspondingTmp).map(data => (data._1, {
          val right = data._2._1.split("[0-9]")
          val answer = data._2._2.split("[0-9]")
          var rightNum = 0
          for (x <- 1 until right.size - 1) if (right(x) == answer(x)) rightNum += 1
          right.size - 1 - rightNum
        })).map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._1._5, data._2), 1))
          .reduceByKey(_ + _)

        val everyCorrespondingMistakeNum = everyCorrespondingMistakeNumRdd.map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._1._5),
          data._1._6 + ":" + data._2))

        /**
          * 下课评分
          */

        val overEvaluationClassRdd = json_name_dataARR.filter(_._1 == "tb_tech_overclass")
          .flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getString("fStr_StudentID"),
          json.getDoubleValue("fFlt_TeacherScore"),
          json.getDoubleValue("fFlt_SelfScore"),
          json.getIntValue("fInt_SubmitType"),
          json.getString("fDtt_AnswerTime")
        )).filter(_._4 == 1).distinct().cache()
        val overEvaluationClassTmp = overEvaluationClassRdd
          .map(data => (data._1, data._5)).groupByKey().mapValues(f => {
          f.toList.sorted.reverse.head
        }).map(data => ((data._1, data._2), 1))
        val overEvaluationClassLast = overEvaluationClassRdd.map(data => (
          (data._1, data._5), (data._2, data._3))
        ).join(overEvaluationClassTmp)
          .map(data => (data._1._1, data._1._2, data._2._1._1, data._2._1._2))
        // 参与评分人数
        val overEvaluationClassNum = overEvaluationClassLast.count
        //对教师评分AVG
        val teacherAVG = (overEvaluationClassLast.map(_._3).reduce(_ + _) /
          overEvaluationClassNum.toDouble).round
        //学生自评AVG
        val stuAVG = (overEvaluationClassLast.map(_._4).reduce(_ + _) /
          overEvaluationClassNum.toDouble).round
        //        println(s"教师平均分:$teacherAVG ")
        //        println(s"学生自评平均分:$stuAVG ")
        val selfLast = overEvaluationClassLast.map(x => (x._1, x._4)).sortBy(_._2)
        //        println("学生自评后三名")
        //        selfLastThree.foreach(println(_))

        val perceptionRdd = json_name_dataARR.filter(_._1 == "tb_tech_perception")
          .flatMap(x => x._2)
          .map(data => JSON.parseObject(data.toString)).map(json => (
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_QuestionNo"),
          json.getString("fStr_StudentID"),
          json.getIntValue("fInt_PerceptionValue"),
          json.getString("fStr_PerceptionName"),
          json.getString("fDtt_AnswerTime")
        )).distinct()
        //  val perceptionRddTmp = perceptionRdd.map(data => ((data._1, data._2, data._3, data._4), data._7)).groupByKey().mapValues(f => {
        //    f.toList.reverse.head
        //  }).map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._2), 1))
        //  val perceptionRddLast = perceptionRdd.map(data => ((data._1, data._2, data._3, data._4, data._7), (data._5, data._6))).join(perceptionRddTmp).distinct()
        //  // 标签前三
        //  perceptionRddLast.foreach(println(_))
        val perceptionTagTop = perceptionRdd
          .map(data => ((data._1, data._2, data._3, data._4, data._7), data._5))
          .distinct()
          .map(data => (data._2, 1))
          .reduceByKey(_ + _)
          .sortBy(_._2, false)
          .take(3)
        val perceptionTagTopThree = perceptionTagTop.map(data => data._1 + ":" + data._2).mkString(",")


        val effect = Array(Tuple18(courseID,
          interact_count,
          loginCount,
          noResponseManCountMsg._1,
          noResponseManCountMsg._2,
          objectivesQuestionAnswerNum.toInt,
          objectivesRightRatio.toInt,
          subjectiveOverRatio,
          subjectiveLastAnswerNum,
          focusRate,
          correctRate,
          teacherAVG,
          overEvaluationClassNum,
          stuAVG,
          overEvaluationClassNum,
          beforeClassRatio,
          afterClassRatio,
          perceptionTagTopThree
        ))
        import spark.implicits._
        val courseDF: DataFrame = sc.parallelize(effect)
          .map(line => tb_teach_msg(line._1, line._2, line._3, line._4,
            line._5, line._6, line._7, line._8.toInt, line._9.toInt,
            line._10.toInt, line._11.toInt, line._12.toInt, line._13.toInt,
            line._14.toInt, line._15.toInt, line._16.toInt, line._17.toInt, line._18))
          .toDF()
        val prop = new Properties()
        prop.setProperty("user", jdbcUserTest)
        prop.setProperty("password", jdbcPasswordTest)
        courseDF.write.mode("append").jdbc(jdbcUrlTest, "tb_teach_msg", prop)

        val stu_rdd = everyAttention.cogroup(everyBodyObjectivesRightRatio, selfLast).mapPartitions(data => {
          data.map(kv => {
            (kv._1, kv._2._1.mkString(","), kv._2._2.mkString(","), kv._2._3.mkString(","))
          })
        })
        val studentDF: DataFrame = stu_rdd.map(line => tb_stu_msg(courseID,
          line._1,
          if (line._2.isEmpty) 0 else line._2.toInt,
          if (line._3.isEmpty) 0 else line._2.toInt,
          if (line._4.isEmpty) 0 else line._2.toInt)).toDF()
        studentDF.write.mode("append").jdbc(jdbcUrlTest, "tb_stu_msg", prop)


        val questionRDD = everyObjectivesRightRatio.union(everySubjectiveOverRatio)
          .cogroup(everyChoiceAnswerNum, everyQuseionNoAnswer, everyCorrespondingMistakeNum)
          .map(data => (data._1,
            data._2._1.mkString(","),
            data._2._2.mkString(","),
            data._2._3.mkString(","),
            data._2._4.mkString(",")))
        val questionDF: DataFrame = questionRDD.map(line => tb_question_msg(courseID, line._1._1,
          line._1._2,
          line._1._3,
          line._1._4,
          line._1._5,
          line._2.toInt,
          line._3,
          line._4,
          line._5
        )).toDF()
        questionDF.write.mode("append").jdbc(jdbcUrlTest, "tb_question_msg", prop)

      }
    })
    unPersistUnUse(Set(), sc)
    ssc
  }


  def show(x: Option[String]): String = x match {
    case Some(s) => s
    case None => "?"
  }

  /**
    * 差值 用户groupBy后，second - first
    *
    * @return
    */
  def diffValue(first: Iterable[Int], second: Iterable[Int]): Int = {
    if (first.nonEmpty) {
      if (second.nonEmpty) {
        second.mkString(",").toInt - first.mkString(",").toInt
      } else {
        -1
      }
    } else {
      second.mkString(",").toInt
    }

  }

  /**
    * 释放RDD缓存
    *
    * @param rddString 不需要释放的RDD
    * @param sc        SparkContext
    */
  def unPersistUnUse(rddString: Set[String], sc: SparkContext): Unit = {
    val persistRDD = sc.getPersistentRDDs
    persistRDD.foreach(tuple => {
      if (!rddString.contains(tuple._2.toString())) {
        tuple._2.unpersist()
      }
    })
  }

  /**
    * 负责启动守护的jetty服务
    *
    * @param port 对外暴露的端口号
    * @param ssc  Stream上下文
    */
  def daemonHttpServer(port: Int, ssc: StreamingContext): Unit = {
    val server = new Server(port)
    val context = new ContextHandler()
    context.setContextPath("/close")
    context.setHandler(new CloseStreamHandler(ssc))
    server.setHandler(context)
    server.start()
  }

  /** * 负责接受http请求来优雅的关闭流
    *
    * @param ssc Stream上下文
    */
  class CloseStreamHandler(ssc: StreamingContext) extends AbstractHandler {
    override def handle(s: String, baseRequest: Request, req: HttpServletRequest, response: HttpServletResponse): Unit = {
      log.warn("开始关闭......")
      ssc.stop(stopSparkContext = true, stopGracefully = true) //优雅的关闭
      response.setContentType("text/html; charset=utf-8")
      response.setStatus(HttpServletResponse.SC_OK)
      val out = response.getWriter
      out.println("close success")
      baseRequest.setHandled(true)
      log.warn("关闭成功.....")
      System.exit(1)
    }
  }

}
