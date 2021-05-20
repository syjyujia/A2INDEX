package a_2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * utility for query data from heap file
 * <p>
 *     java dbquery querytext pagesize
 *     e.g. java dbquery sensor_id=67 heap.4096
 * </p>
 * @see Key
 */
public class dbqueryWithIndex {

    public static void main(String[] args) {

        if (args == null || args.length != 2) {
            System.err.println("java dbquery querytext heapfile");
            return;
        }

        String condition = args[0];
        String heapFilePath = args[1];

        String[] conditionArr = condition.split("=");
        Key column = Key.valueOf(conditionArr[0]);
        String value = conditionArr[1];

        boolean useIndex = false;
        File indexFile = new File(heapFilePath + "." + column.name());
        if (indexFile.exists()) {
            useIndex = true;
        }

        Map<Integer, List<Integer>> pageRecords = null;
        if (useIndex) {
            List<Record> records;
            if (column.indexType == Integer.class) {
                BPlusTree<Integer, Record> bPlusTree = BPlusTree.deserialize(heapFilePath, column);
                records = bPlusTree.search(Integer.valueOf(value));
            } else {
                BPlusTree<String, Record> bPlusTree = BPlusTree.deserialize(heapFilePath, column);
                records = bPlusTree.search(value);
            }

            pageRecords = records.stream()
                    .collect(Collectors.groupingBy(Record::getPageNumber, Collectors.mapping(Record::getRecordOffset, Collectors.toList())));
        }

        String queryText = args[0];

        FileInputStream fileInputStream = null;
        try {

            long start = System.currentTimeMillis();

            // open heap file
            File heapFile = new File(heapFilePath);
            fileInputStream = new FileInputStream(heapFile);
            String[] heapFileArr = heapFilePath.split("\\.");
            int pageSize = Integer.valueOf(heapFileArr[heapFileArr.length - 1]);

            // initialize byte buffer
            byte[] buffer = new byte[pageSize];

            // total count of records matching the query text
            int totalCount = 0;

            // read the heap file
            int pageNumber = 0;
            while (fileInputStream.read(buffer) >= 0) {
                pageNumber ++;
                if (useIndex && !pageRecords.containsKey(pageNumber)) {
                    continue;
                }

                if (useIndex) {
                    List<Integer> recordNumbers = pageRecords.get(pageNumber).stream().sorted(Integer::compareTo).collect(Collectors.toList());
                    // find matched data in current page data
                    totalCount += pageQuery(buffer, recordNumbers, pageSize);
                } else {
                    totalCount += pageQueryWithoutIndex(buffer, column, value);
                }
            }

            long end = System.currentTimeMillis();
            System.out.println("find " + totalCount + " records in " + (end - start) + "ms.");

        } catch (Exception e) {
            System.err.println("open heap file failed");
            e.printStackTrace();
        } finally {
            try {
                fileInputStream.close();
            } catch (IOException e) {
                System.err.println("close file input stream failed");
                e.printStackTrace();
            }
        }
    }

    /**
     * query in current page
     * @param pageData page data
     */
    private static int pageQueryWithoutIndex(byte[] pageData, Key key, String value) {

        // record count matches query text
        int recordCount = 0;

        // the first record starts from the fourth byte of the page, exclude page record count
        int recordStartIndex = 4;

        while (true) {
            if (recordStartIndex + 4 >= pageData.length) {
                break;
            }

            // id: 0th~3rd bytes
            int id = Util.byteArrayToInt(Util.getSubArray(pageData, recordStartIndex, 4));
            if (id == 0) {
                // reach end of page
                break;
            }

            boolean match = false;
            if (key == Key.id && id == Integer.valueOf(value)) {
                match = true;
            }
            String dateTime = new String(Util.getSubArray(pageData, recordStartIndex + 4, 10));
            if (key == Key.date_time && dateTime.equals(value)) {
                match = true;
            }
            int year = Util.byteArrayToInt(Util.getSubArray(pageData, recordStartIndex + 14, 4));
            if (key == Key.year && year == Integer.valueOf(value)) {
                match = true;
            }
            int month = Byte.valueOf(pageData[recordStartIndex + 18]).intValue();
            if (key == Key.month && month == Integer.valueOf(value)) {
                match = true;
            }
            int mdate = Byte.valueOf(pageData[recordStartIndex + 19]).intValue();
            if (key == Key.mdate && mdate == Integer.valueOf(value)) {
                match = true;
            }
            int day = Byte.valueOf(pageData[recordStartIndex + 20]).intValue();
            if (key == Key.day && day == Integer.valueOf(value)) {
                match = true;
            }
            int time = Byte.valueOf(pageData[recordStartIndex + 21]).intValue();
            if (key == Key.time && time == Integer.valueOf(value)) {
                match = true;
            }
            int sensorId = Util.byteArrayToInt(Util.getSubArray(pageData, recordStartIndex + 22, 4));
            if (key == Key.sensor_id && sensorId == Integer.valueOf(value)) {
                match = true;
            }
            int hourlyCounts = Util.byteArrayToInt(Util.getSubArray(pageData, recordStartIndex + 26, 4));
            if (key == Key.hourly_counts && hourlyCounts == Integer.valueOf(value)) {
                match = true;
            }

            // sensor_name_length: 20th~24th bytes
            int sensorNameLength = Util.byteArrayToInt(Util.getSubArray(pageData, recordStartIndex + 30, 4));
            // Sensor_Name
            String sensorName = new String(Util.getSubArray(pageData, recordStartIndex + 34, sensorNameLength));
            // SDT_NAME start position
            int sdtNameIndex = recordStartIndex + 34 + sensorNameLength;
            // convert SDT_NAME_length bytes to int value
            int sdtNameLength = Util.byteArrayToInt(Util.getSubArray(pageData, sdtNameIndex, 4));
            // SDT_NAME string
            String sdtName = new String(Util.getSubArray(pageData, sdtNameIndex + 4, sdtNameLength));

            if (key == Key.sensor_name && sensorName.equals(value)) {
                match = true;
            }
            if (key == Key.sdt_name && sdtName.equals(value)) {
                match = true;
            }

            // data match
            if (match) {
                System.out.println("ID=" + id + ", Sensor_Id=" + sensorId + ", Sensor_Name=" + sensorName
                        + ", SDT_NAME=" + sdtName + ", Hourly_Counts=" + hourlyCounts);
                recordCount ++;
            }

            // move to the start index of next record
            recordStartIndex += (38 + sensorNameLength + sdtNameLength);
        }

        return recordCount;
    }

    /**
     * query in current page
     * @param pageData page data
     * @param recordNumbers record index in page
     * @param pageSize
     */
    private static int pageQuery(byte[] pageData, List<Integer> recordNumbers, Integer pageSize) {

        // record count matches query text
        int recordCount = 0;

        // the first record starts from the fourth byte of the page, exclude page record count
//        int recordStartIndex = 4;

        for (Integer recordStartIndex : recordNumbers) {
            if (recordStartIndex + 4 >= pageSize) {
                break;
            }

            // id: 0th~3rd bytes
            int id = Util.byteArrayToInt(Util.getSubArray(pageData, recordStartIndex, 4));
            if (id == 0) {
                // reach end of page
                break;
            }

            // sensor_name_length: 20th~24th bytes
            int sensorNameLength = Util.byteArrayToInt(Util.getSubArray(pageData, recordStartIndex + 30, 4));
            // Sensor_Name
            String sensorName = new String(Util.getSubArray(pageData, recordStartIndex + 34, sensorNameLength));
            // SDT_NAME start position
            int sdtNameIndex = recordStartIndex + 34 + sensorNameLength;
            // convert SDT_NAME_length bytes to int value
            int sdtNameLength = Util.byteArrayToInt(Util.getSubArray(pageData, sdtNameIndex, 4));
            // SDT_NAME string
            String sdtName = new String(Util.getSubArray(pageData, sdtNameIndex + 4, sdtNameLength));

            // sensor_id: 12th~15th bytes
            int sensorId = Util.byteArrayToInt(Util.getSubArray(pageData, recordStartIndex + 22, 4));
            // Hourly_Counts: 16th~19th bytes
            int hourlyCounts = Util.byteArrayToInt(Util.getSubArray(pageData, recordStartIndex + 26, 4));

            // data match
//            if (recordNumbers.equals(sdtName)) {
                System.out.println("ID=" + id + ", Sensor_Id=" + sensorId + ", Sensor_Name=" + sensorName
                        + ", SDT_NAME=" + sdtName + ", Hourly_Counts=" + hourlyCounts);
                recordCount ++;
//            }

            // move to the start index of next record
//            recordStartIndex += (38 + sensorNameLength + sdtNameLength);
        }

        return recordCount;
    }
//
//    /**
//     * get the sub array of bytes from startIndex to startIndex + length
//     * @param bytes the base type array
//     * @param startIndex the start index of base array
//     * @param length the length of sub array
//     * @return
//     */
//    private static byte[] getSubArray(byte[] bytes, int startIndex, int length) {
//        byte[] result = new byte[length];
//        for (int i = 0; i < length; i++) {
//            result[i] = bytes[startIndex + i];
//        }
//        return result;
//    }
//
//    /**
//     * transfer a byte array data into an int value
//     * @param bytes the original byte array
//     * @return the converted int value
//     */
//    public static int byteArrayToInt(byte[] bytes) {
//        int value = 0;
//        for (int i = 0; i < 4; i++) {
//            int shift = (3 - i) * 8;
//            value += (bytes[i] & 0xFF) << shift;
//        }
//        return value;
//    }
}
