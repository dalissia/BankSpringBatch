package com.id.bank_spring_batch;


import com.id.bank_spring_batch.dao.BankTransaction;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;


@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
    @Autowired  private JobBuilderFactory  jobBuilderFactory ;
    @Autowired private StepBuilderFactory stepBuilderFactory;
    @Autowired private ItemReader<BankTransaction> bankTransactionItemReader;
    @Autowired private ItemWriter<BankTransaction> bankTransactionItemWriter;
    @Autowired private ItemProcessor<BankTransaction, BankTransaction> bankTransactionItemProcessor;

    @Bean
    public Job bankJob(){
        // Définition d'une étape (Step) du job
        Step step1 = stepBuilderFactory.get("step-load-data")
                .<BankTransaction,BankTransaction> chunk(100)   // Spécifie le type d'entrée et de sortie, et le nombre d'éléments à traiter par chunk (100)
                .reader(bankTransactionItemReader) // Définit le composant pour lire les données
                // .processor(bankTransactionItemProcessor)
                .processor(compositeItemProcessor()) // Utilise un processeur composite pour traiter les données
                .writer(bankTransactionItemWriter) // Définit le composant pour écrire les données
                .build();
        // Construit et retourne le job en démarrant avec la première étape
        return jobBuilderFactory.get("bank-data-loader-job")
                .start(step1).build();
    }

    // Définition d'un processeur composite (CompositeItemProcessor)
    @Bean
     public  ItemProcessor<BankTransaction,  BankTransaction> compositeItemProcessor(){
        List<ItemProcessor <BankTransaction, BankTransaction>> itemProcessors = new ArrayList<>();
        itemProcessors.add(itemProcessor1()); // Ajoute le premier processeur qui formate la date
        itemProcessors.add(itemProcessor2()); // Ajoute le second processeur qui effectue des calculs d'analytique
        CompositeItemProcessor<BankTransaction,BankTransaction> compositeItemProcessor = new CompositeItemProcessor<>();
        compositeItemProcessor.setDelegates(itemProcessors);  // Définit la liste des processeurs délégués

        return compositeItemProcessor;
    }

    // Définition du premier processeur qui formate la date
    @Bean
    public BankTransactionItemProcessor itemProcessor1() {
        return new BankTransactionItemProcessor(); // ce processor va convertir la date au bon format
    }

    // Définition du second processeur qui calcule le total des crédits et débits
    @Bean
    public BankTransactionItemAnalyticsProcessor itemProcessor2() {
        return new BankTransactionItemAnalyticsProcessor();
    }

    // Définition du lecteur de fichier plat (FlatFileItemReader) pour lire les données du fichier d'entrée
    @Bean
    public FlatFileItemReader<BankTransaction> flatFileItemReader (@Value("${inputFile}") Resource inputFile){
        // L'annotation @Value permet d'injecter une valeur provenant du fichier application.properties
        FlatFileItemReader<BankTransaction> fileItemReader =  new FlatFileItemReader<BankTransaction>();
        fileItemReader.setName("FFIR1");
        fileItemReader.setLinesToSkip(1);
        fileItemReader.setResource(inputFile);
        fileItemReader.setLineMapper(lineMapper());
        return fileItemReader;
    }

    // Définit le mappage de chaque ligne du fichier à un objet Java (BankTransaction)
    public LineMapper<BankTransaction> lineMapper() {
        DefaultLineMapper<BankTransaction> lineMapper = new DefaultLineMapper<BankTransaction>();
        DelimitedLineTokenizer lineTokenizer =  new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id","accountID","strTransactionDate","transactionType","amount");
        lineMapper.setLineTokenizer(lineTokenizer);
        BeanWrapperFieldSetMapper<BankTransaction> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(BankTransaction.class);  // Définit la classe cible pour chaque ligne mappée
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }
}

