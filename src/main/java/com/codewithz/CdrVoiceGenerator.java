package com.codewithz;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class CdrVoiceGenerator {

    private static final String OUTPUT_DIR = "C:\\Voice-Point-Actual";
    private static final int RECORDS_PER_FILE = 10000;
    private static final int FILES_PER_BATCH = 5;
    private static final int BATCHES = 6;

    private static final Random RANDOM = new Random();
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter FILE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    // Shared dictionaries
    private static final String[] OPERATORS = {"Etisalat", "Orange", "Vodacom", "Airtel"};
    private static final String[] SUBSCRIBER_TYPES = {"Prepaid", "Postpaid"};
    private static final String[] COUNTRIES = {"UAE", "Kenya", "Tanzania", "Egypt"};
    private static final String[] OFFER_NAMES = {"VoicePack", "SuperTalk", "InternetPack", "IntlCall"};
    private static final String[] HUAWEI_OFFERS = {"HOfferA", "HOfferB", "HOfferC"};

    // Reusable IDs for joins across files
    private static final List<String> SHARED_MSISDNs = Arrays.asList(
            "+971500000001", "+971500000002", "+971500000003", "+971500000004"
    );
    private static final List<String> SHARED_IMSIs = Arrays.asList(
            "420010000000001", "420010000000002", "420010000000003"
    );

    public static void main(String[] args) {
        File dir = new File(OUTPUT_DIR);
        if (!dir.exists()) dir.mkdirs();

        // Define the date range
        LocalDate startDate = LocalDate.of(2025, 9, 8);
        LocalDate endDate = LocalDate.of(2025, 9, 11);

        LocalDate currentDate = startDate;
        while (!currentDate.isAfter(endDate)) {
            System.out.println("Generating data for " + currentDate);

            for (int batch = 1; batch <= BATCHES; batch++) {
                for (int i = 0; i < FILES_PER_BATCH; i++) {
                    String timestamp = LocalDateTime.now().format(FILE_FORMATTER);
                    String filename = "cdr_" + currentDate + "_batch" + batch +
                            "_file" + (i + 1) + "_" + timestamp + ".ndjson";

                    File file = new File(dir, filename);

                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                        for (int j = 0; j < RECORDS_PER_FILE; j++) {
                            ObjectNode record = generateRecord(j, filename, currentDate);
                            writer.write(MAPPER.writeValueAsString(record));
                            writer.newLine();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Created file: " + file.getAbsolutePath());
                }
            }

            currentDate = currentDate.plusDays(1); // move to next day
        }
    }

    private static ObjectNode generateRecord(int recordNum, String filename, LocalDate day) {
        ObjectNode data = MAPPER.createObjectNode();

        // Random time during this day
        LocalDateTime start = day.atStartOfDay().plusMinutes(RANDOM.nextInt(1440));
        LocalDateTime end = start.plusMinutes(RANDOM.nextInt(30));

        String msisdn = RANDOM.nextInt(10) == 0 ?
                SHARED_MSISDNs.get(RANDOM.nextInt(SHARED_MSISDNs.size())) :
                "+9715" + (10000000 + RANDOM.nextInt(89999999));

        String imsi = RANDOM.nextInt(10) == 0 ?
                SHARED_IMSIs.get(RANDOM.nextInt(SHARED_IMSIs.size())) :
                "42001" + (100000000 + RANDOM.nextInt(899999999));

        data.put("RecordType", "VOICE");
        data.put("OfferID", "OFF-" + RANDOM.nextInt(10000));
        data.put("BalanceID", "BAL-" + RANDOM.nextInt(5000));
        data.put("MSISDN", msisdn);
        data.put("IMSI", imsi);
        data.put("SubscriberTypeCode", RANDOM.nextBoolean() ? "P" : "C");
        data.put("SubscriberType", SUBSCRIBER_TYPES[RANDOM.nextInt(SUBSCRIBER_TYPES.length)]);
        data.put("IsCharged", RANDOM.nextBoolean());
        data.put("SGSNIPAddress", "10.0." + RANDOM.nextInt(255) + "." + RANDOM.nextInt(255));
        data.put("GGSNAddress", "172.16." + RANDOM.nextInt(255) + "." + RANDOM.nextInt(255));
        data.put("GGSNChargingID", "CHG-" + RANDOM.nextInt(100000));
        data.put("IsRoaming", RANDOM.nextBoolean());
        data.put("DataVolumeGPRSTotal", RANDOM.nextInt(1000000));
        data.put("DataVolumeGPRSDownlink", RANDOM.nextInt(500000));
        data.put("DataVolumeGPRSUplink", RANDOM.nextInt(500000));
        data.put("OfferName", OFFER_NAMES[RANDOM.nextInt(OFFER_NAMES.length)]);
        data.put("RoamingCountry", COUNTRIES[RANDOM.nextInt(COUNTRIES.length)]);
        data.put("RatedCashValue", RANDOM.nextDouble() * 100);
        data.put("HuaweiOfferID", "HUA-" + RANDOM.nextInt(1000));
        data.put("HuaweiOfferName", HUAWEI_OFFERS[RANDOM.nextInt(HUAWEI_OFFERS.length)]);
        data.put("PrimaryOfferID", "PRIM-" + RANDOM.nextInt(100));
        data.put("RecordOpeningTime", start.format(FORMATTER));
        data.put("RecordClosingTime", end.format(FORMATTER));
        data.put("TotalCharge", RANDOM.nextDouble() * 10);
        data.put("UsedUnits", RANDOM.nextInt(500));
        data.put("Operator", OPERATORS[RANDOM.nextInt(OPERATORS.length)]);
        data.put("FileName", filename);
        data.put("DateParsed", day.atStartOfDay().format(FORMATTER));

        return data;
    }
}
