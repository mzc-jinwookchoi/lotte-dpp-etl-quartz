package com.sene;


public class EtlMainProcess {
    public static void main(String[] args) {
//        SchedulerFactory schedFact = new StdSchedulerFactory();
        EtlBatchClass etlBatchClass = new EtlBatchClass();
        try {
            etlBatchClass.EtlProcess();
        }catch (Exception e){
            e.printStackTrace();
        }

//        try {
//            Scheduler sched = schedFact.getScheduler();
//            JobDetail jobA = JobBuilder.newJob(EtlBatchClass.class).withIdentity("jobA", "group2").build();
//
////            String cronExp = "0 0 6 * * ?"; // 운영 매일 오전 6시
//            String cronExp = "0/30 * * * * ?"; // 매 30초 간격
////            String cronExp = "0 0/1 * * * ?"; // 매 1분 간격
////            String cronExp = "0/30 * * * * ?"; // 매 30초 간격
////            String cronExp = "0 0 0/1 * * ?"; // 매 1시간 간격
//
//            // CronTrigger 생성
//            CronScheduleBuilder cronSch = CronScheduleBuilder.cronSchedule(new CronExpression(cronExp));
//            CronTrigger triggerA = (CronTrigger) TriggerBuilder.newTrigger()
//                    .withIdentity("triggerA", "group2")
//                    .withSchedule(cronSch)
//                    .forJob(jobA)
//                    .build();
//
//            sched.scheduleJob(jobA, triggerA);
//            sched.start();
//
//        } catch (SchedulerException e) {
//            e.printStackTrace();
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
    }
}
