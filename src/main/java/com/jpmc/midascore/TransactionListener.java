
package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import com.jpmc.midascore.foundation.Incentive;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate = new RestTemplate();

    public TransactionListener(
            UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository
    ) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @Transactional
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void listen(Transaction transaction) {
        Optional<UserRecord> senderOptional = userRepository.findById(Long.valueOf(transaction.getSenderId()));
        Optional<UserRecord> recipientOptional = userRepository.findById(Long.valueOf(transaction.getRecipientId()));
        if (senderOptional.isEmpty() || recipientOptional.isEmpty()) {
            return;
        }

        UserRecord sender = senderOptional.get();
        UserRecord recipient = recipientOptional.get();

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
        );

        float incentiveAmount = incentive.getAmount();

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        TransactionRecord transactionRecord = new TransactionRecord(
                sender,
                recipient,
                transaction.getAmount(),
                incentiveAmount
        );

        transactionRecordRepository.save(transactionRecord);
        userRepository.save(sender);
        userRepository.save(recipient);

        if ("wilbur".equals(sender.getName())) {
            System.out.println("WILBUR BALANCE: " + sender.getBalance());
        }

        if ("wilbur".equals(recipient.getName())) {
            System.out.println("WILBUR BALANCE: " + recipient.getBalance());
        }
    }
}