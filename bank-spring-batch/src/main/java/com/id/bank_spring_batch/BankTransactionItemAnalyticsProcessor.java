package com.id.bank_spring_batch;

import com.id.bank_spring_batch.dao.BankTransaction;
import org.springframework.batch.item.ItemProcessor;
import lombok.Getter;

public class BankTransactionItemAnalyticsProcessor  implements ItemProcessor<BankTransaction, BankTransaction> {

        @Getter
        private  double totalDebit;
        @Getter
        private double totalCredit;

        @Override
    public BankTransaction process(BankTransaction bankTransaction) throws  Exception{
            if(bankTransaction.getTransactionType().equals("D"))
                totalDebit += bankTransaction.getAmount();
            else  if (bankTransaction.getTransactionType().equals("C"))
                totalCredit += bankTransaction.getAmount();
            return  bankTransaction;
        }
}
