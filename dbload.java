package a_2;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.format.DateTimeFormatter;

/**
 * utility for loading data into heap file
 * <p>
 *     java dbload -p pagesize filepath
 *     e.g. java dbload -p 2048 ./source.csv
 * </p>
 */
public class dbload {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddhh");

    public static void main(String[] args) {

        Integer pageSize = Integer.parseInt(args[1]);
        String sourceFilePath = args[2];

        /* open input and output file */
        BufferedReader bufferedReader = null;
        BufferedOutputStream bufferedOutputStream = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(sourceFilePath));
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("heap." + pageSize));
        } catch (IOException e) {
            System.err.println("open file failed");
            e.printStackTrace();
        }

        long startTime = System.currentTimeMillis();

        // row content
        String row;
        // page buffer
        byte[] buffer = new byte[pageSize];
        // page buffer writing cursor, 4 bytes of page record count value reserved
        Integer cursor = 4;
        // page record count
        int pageRecordCount = 0;
        // row number
        int rowNumber = 0;

        // page number
        int pageNumber = 0;

        try {
            while (true) {
                row = bufferedReader.readLine();
                if (row == null) {
                    // the end of the stream has been reached
                    break;
                }
                rowNumber ++;

                if (rowNumber == 1) {
                    // skip header
                    continue;
                }

                // get bytes data of row content
                byte[] rowData = getRowData(row);

                // current page will overflow if this row is appended to this page, write current page and open a new page
                if (cursor + rowData.length > pageSize) {
                    // write page data
                    writePage(pageSize, bufferedOutputStream, buffer, cursor, pageRecordCount);
                    pageNumber ++;

                    // reset page buffer
                    buffer = new byte[pageSize];
                    pageRecordCount = 0;
                    cursor = 4;
                }

                // append row data to page buffer
                copyArray(buffer, rowData, cursor);
                // move the page buffer cursor
                cursor += rowData.length;
                // page record count increment
                pageRecordCount ++;
            }

            if (pageRecordCount > 0) {
                // write the last page
                writePage(pageSize, bufferedOutputStream, buffer, cursor, pageRecordCount);
                pageNumber ++;
            }

            long endTime = System.currentTimeMillis();
            System.out.println("write " + (rowNumber - 1) + " records, " + pageNumber + " pages into heap file, cost " + (endTime - startTime) + "ms.");
        } catch (Exception e) {
            System.err.println("write page data error");
            e.printStackTrace();
        } finally {
            try {
                bufferedReader.close();
                bufferedOutputStream.close();
            } catch (IOException e) {
                System.err.println("release file resource failed");
                e.printStackTrace();
            }
        }
    }

    /**
     * write page data to output heap file
     * @param pageSize heap file page size
     * @param bufferedOutputStream output stream
     * @param buffer page buffer
     * @param cursor cursor of page buffer
     * @param pageRecordCount current page record count
     * @throws IOException
     */
    private static void writePage(Integer pageSize,
                                  BufferedOutputStream bufferedOutputStream,
                                  byte[] buffer,
                                  Integer cursor,
                                  int pageRecordCount) throws IOException {
        // write page record count into buffer
        copyArray(buffer, intToByteArray(pageRecordCount), 0);

        // put 0 into tailing empty space
        for (int i = cursor; i < pageSize; i++) {
            buffer[i] = (byte) 0;
        }

        // write the page data
        bufferedOutputStream.write(buffer);
        bufferedOutputStream.flush();

        System.out.println("page saved with " + pageRecordCount + " records.");
    }

    /**
     * transfer row content to bytes
     * @param row
     * @return
     */
    private static byte[] getRowData(String row) {
        // row content split
        String[] rowDataArray = row.split(",");

        /* convert data */
        Integer id = Integer.valueOf(rowDataArray[0]);
        String sdtName =rowDataArray[1] + "_" + rowDataArray[7];
        Integer year = Integer.valueOf(rowDataArray[2]);
        byte month = (byte) Month.valueOf(rowDataArray[3].toUpperCase()).getValue();
        byte mdate = Byte.valueOf(rowDataArray[4]);
        byte day = (byte) DayOfWeek.valueOf(rowDataArray[5].trim().toUpperCase()).getValue();
        byte time = Byte.valueOf(rowDataArray[6]);
        Integer sensorId = Integer.valueOf(rowDataArray[7]);
        String sensorName = rowDataArray[8];
        Integer hourlyCounts = Integer.valueOf(rowDataArray[9]);
        LocalDateTime dateTime = LocalDateTime.of(year, month, mdate, time, 0);

        /* transfer primitive type to bytes */
        byte[] dateTimeBytes = dateTime.format(DATE_TIME_FORMATTER).getBytes();
        byte[] idBytes = intToByteArray(id);
        byte[] yearBytes = intToByteArray(year);
        byte[] sensorIdBytes = intToByteArray(sensorId);
        byte[] hourlyCountsBytes = intToByteArray(hourlyCounts);
        byte[] sdtNameBytes = sdtName.getBytes();
        byte[] sensorNameBytes = sensorName.getBytes();

        /* concat all fields bytes together */
        byte[] rowData = new byte[38 + sensorNameBytes.length + sdtNameBytes.length];
        copyArray(rowData, idBytes, 0);
        copyArray(rowData, dateTimeBytes, 4);
        copyArray(rowData, yearBytes, 14);
        rowData[18] = month;
        rowData[19] = mdate;
        rowData[20] = day;
        rowData[21] = time;
        copyArray(rowData, sensorIdBytes, 22);
        copyArray(rowData, hourlyCountsBytes, 26);
        copyArray(rowData, intToByteArray(sensorNameBytes.length), 30);
        copyArray(rowData, sensorNameBytes, 34);
        copyArray(rowData, intToByteArray(sdtNameBytes.length), 34 + sensorNameBytes.length);
        copyArray(rowData, sdtNameBytes, 38 + sensorNameBytes.length);

        return rowData;
    }

    /**
     * concat subArray data into array
     * @param array
     * @param subArray
     * @param startIndex the start index of array where surArray data is set
     */
    private static void copyArray(byte[] array, byte[] subArray, int startIndex) {
        for (int i = 0; i < subArray.length; i++) {
            array[startIndex + i] = subArray[i];
        }
    }

    /**
     * transfer an int value to byte array
     * @param i the original int value
     * @return byte array of int value
     */
    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        result[0] = (byte)((i >> 24) & 0xFF);
        result[1] = (byte)((i >> 16) & 0xFF);
        result[2] = (byte)((i >> 8) & 0xFF);
        result[3] = (byte)(i & 0xFF);
        return result;
    }
}
