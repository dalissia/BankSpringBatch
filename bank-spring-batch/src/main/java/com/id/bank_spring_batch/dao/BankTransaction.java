package com.id.bank_spring_batch.dao;
import java.util.Date;
;import java.util.Date;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Transient;


@Entity
@Data @NoArgsConstructor @AllArgsConstructor @ToString
public class BankTransaction {

    @Id
    private Long id;
    private long accountID;
    private Date transactionDate;
    @Transient
    private String strTransactionDate;
    private String transactionType;
    private double amount;



}