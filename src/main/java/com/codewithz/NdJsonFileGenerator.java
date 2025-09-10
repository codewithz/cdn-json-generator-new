package com.codewithz;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

public class NdJsonFileGenerator {

    private static final String OUTPUT_DIR = "C:\\Data-Point";
    private static final int RECORDS_PER_FILE = 100;
    private static final int FILES_PER_BATCH = 5;
    private static final int INTERVAL_MS = 10000; // 10 seconds
    private static final Random RANDOM = new Random();
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter FILE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private static final String[] OPERATORS = {"Etisalat", "Orange", "Vodacom", "Airtel"};

    public static void main(String[] args) throws InterruptedException {
        File dir = new File(OUTPUT_DIR);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        int batch = 1;
        while (batch <= 5) {
            System.out.println("Generating batch " + batch + " ...");
            for (int i = 0; i < FILES_PER_BATCH; i++) {
                // ? Generate unique filename with timestamp + UUID
                String timestamp = LocalDateTime.now().format(FILE_FORMATTER);
                String uniqueId = UUID.randomUUID().toString();
                String filename = "records_batch" + batch + "_file" + (i + 1) + "_" + timestamp + "_" + uniqueId + ".ndjson";

                File file = new File(dir, filename);

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                    for (int j = 0; j < RECORDS_PER_FILE; j++) {
                        ObjectNode record = generateRecord(j, filename);
                        writer.write(MAPPER.writeValueAsString(record));
                        writer.newLine();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("Created file: " + file.getAbsolutePath());
            }

            batch++;
            Thread.sleep(INTERVAL_MS);
        }
    }

    private static ObjectNode generateRecord(int recordNum, String filename) {
        ObjectNode data = MAPPER.createObjectNode();

        data.put("Record Type", "MOC");
        data.put("Calling Number", "+9715" + (10000000 + RANDOM.nextInt(89999999)));
        data.put("Called Number", "+9714" + (1000000 + RANDOM.nextInt(8999999)));
        data.put("Recording Entity", "MSC" + RANDOM.nextInt(100));
        data.put("MSC Incoming Route", "Route-In-" + RANDOM.nextInt(50));
        data.put("MSC Outgoing Route", "Route-Out-" + RANDOM.nextInt(50));

        data.put("Seizure Time", LocalDateTime.now().minusSeconds(RANDOM.nextInt(1000)).format(FORMATTER));
        data.put("Answer Time", LocalDateTime.now().format(FORMATTER));
        data.put("Release Time", LocalDateTime.now().plusSeconds(RANDOM.nextInt(300)).format(FORMATTER));
        data.put("Call Duration", RANDOM.nextInt(500));
        data.put("Diagnostics", "OK");
        data.put("Call Reference", "REF" + RANDOM.nextInt(100000));
        data.put("Basic Service", "Voice");
        data.put("Roaming Number", "+97150" + RANDOM.nextInt(9999999));
        data.put("MSC Outgoing Circuit", "CIR" + RANDOM.nextInt(200));
        data.put("Originating MSC ID", "MSC-ID-" + RANDOM.nextInt(100));
        data.put("Filename", filename);
        data.put("Parsed Time", LocalDateTime.now().format(FORMATTER));
        data.put("Operator", OPERATORS[RANDOM.nextInt(OPERATORS.length)]);

        data.put("IMSI", "42001" + RANDOM.nextInt(999999999));
        data.put("IMEI", "356" + RANDOM.nextInt(999999999));
        data.put("MSISDN", "+97152" + RANDOM.nextInt(9999999));
        data.put("MS Classmark", "Class-" + RANDOM.nextInt(10));
        data.put("Location Area Code (LAC)", String.valueOf(1000 + RANDOM.nextInt(9999)));
        data.put("Cell Identifier", String.valueOf(20000 + RANDOM.nextInt(9999)));
        data.put("System Type", "GSM");
        data.put("Basic Service Type", "Tele");
        data.put("Basic Service Code", "TS11");
        data.put("Charged Party", "A");
        data.put("Originating RNC/BSC ID", "BSC-" + RANDOM.nextInt(100));
        data.put("Record Number", recordNum);
        data.put("Translated Number", "+97155" + RANDOM.nextInt(9999999));
        data.put("Call Type", "National");
        data.put("MSC Address", "192.168." + RANDOM.nextInt(255) + "." + RANDOM.nextInt(255));

        return data;
    }
}
