package com.cj.spark.streaming.models

case class tb_teach_msg(v_tid:String,
                  ui_interaction_num:Int,
                  ui_student_sign:Int,
                  ui_no_response:Int,
                  ui_no_response_count:Int,
                  ui_objective_question_num:Int,
                  ui_objective_question_right_ratio:Int,
                  ui_subjective_question_over_ratio:Int,
                  ui_subjective_question_num:Int,
                  focusRate:Int,
                  correctRate:Int,
                  class_mark_avg:Int,
                  class_mark_stu_num:Int,
                  oneself_mark_avg:Int,
                  oneself_mark_num:Int,
                  before_class_ratio:Int,
                  after_class_ratio:Int,
                  perception_tag_three:String
                 )
case class tb_stu_msg(v_tid:String,
                      ui_student_id:String,
                      stu_focusRate:Int,
                      objective_correctRate:Int,
                      self_mark:Int)

case class tb_question_msg(v_tid:String,
                           ui_process_no:Int,
                           ui_interaction_no:Int,
                           ui_question_id:Int,
                           ui_subquestion_no:Int,
                           ui_question_type:Int,
                           focusRate_correctRate:Int,
                           every_choice_Num:String,
                           no_answer_stu:String,
                           everyCorresponding_mistake_Num:String)

class Tables{

}