package com.functions;

import java.util.UUID;

public class EcrAvroToCsv {
    // private static final String prefix = "wks.cim.sfcs/sfctransaction/";

    private static final String uuidKey = "11111";
    static final StringBuffer coverPageBuffer = new StringBuffer();
    static final StringBuffer itemBuffer = new StringBuffer();
    static final StringBuffer logBuffer = new StringBuffer();

    public static void main(final String[] args) {

        // File f = new File("/Users/louis/Downloads/sfc_transaction.avro");

        // readAvro(f);
        // processAvroFile();

    }

    public static void processAvroFile(byte[] content) {

        // final List<GenericRecord> result = new ArrayList<GenericRecord>();
        // parseAvroMessage(content, result);
        // // append into csv file
        // for (final GenericRecord genericRecord : result) {
        // final String uuid = UUID.randomUUID().toString();
        // final ByteBuffer b = (ByteBuffer) genericRecord.get("Body");
        // final SeekableByteArrayInput seekInput = new
        // SeekableByteArrayInput(b.array());
        // readAvro(uuid, seekInput);
        // }
        coverPageBuffer.append(UUID.randomUUID().toString());
        itemBuffer.append(UUID.randomUUID().toString());
        logBuffer.append(UUID.randomUUID().toString());

        try {
            DataLakeUtils.appendFile("partition-data.csv", "ext.wistron.kafka.avro", "Coverpage",
                    coverPageBuffer.toString());

            DataLakeUtils.appendFile("partition-data.csv", "ext.wistron.kafka.avro", "Affecteditem",
                    itemBuffer.toString());

            DataLakeUtils.appendFile("partition-data.csv", "ext.wistron.kafka.avro", "Approvallog",
                    logBuffer.toString());

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}