package com.id.bank_spring_batch;


import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

        @RestController
        public class JobRestController {

                @Autowired
                private JobLauncher jobLauncher;
                @Autowired
                private Job job;
                @Autowired
                private BankTransactionItemAnalyticsProcessor analyticsProcessor;

                //  démarrer un job Spring Batch et de surveiller son statut.
                @GetMapping("/startJob")
                public BatchStatus load() throws Exception {
                    Map<String, JobParameter> params = new HashMap<>();
                    params.put("time",new JobParameter(System.currentTimeMillis()));
                    JobParameters jobParameters  = new JobParameters(params);
                    JobExecution jobExecution = jobLauncher.run(job, jobParameters);
                    while(jobExecution.isRunning()){
                        System.out.println("........");
                    }
                    return  jobExecution.getStatus();
                }

                 //  fournit des résultats d'analyse après que le job a été exécuté, en utilisant les données accumulées par le BankTransactionItemAnalyticsProcessor
                @GetMapping("/analytics")
            public Map<String, Double> analytics (){
                    Map<String, Double> map = new HashMap<>();
                    map.put("totalCredit",analyticsProcessor.getTotalCredit());
                    map.put("totalDebit",analyticsProcessor.getTotalDebit());

                    return map;
                }
        }




