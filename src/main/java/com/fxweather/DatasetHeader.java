package com.fxweather;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

/** Specify the header and column index of a dataset.
 * not following the rules of zero-based indexing, this data was mapped from README.txt in the Datasets folder
 * @author Ramadhan Kalih Sewu
 * @version 0.1
*/
public enum DatasetHeader
{
    WBANNO(1,5,DatasetHeader::parseInt),
    UTC_DATE(7,14,DatasetHeader::parseDate),
    UTC_TIME(16,19,DatasetHeader::parseTime),
    LST_DATE(21,28,DatasetHeader::parseDate),
    LST_TIME(30,33,DatasetHeader::parseTime),
    CRX_VN(35,40,null),
    LONGITUDE(42,48,DatasetHeader::parseFloat),
    LATITUDE(50,56,DatasetHeader::parseFloat),
    T_CALC(58,64,DatasetHeader::parseFloat),
    T_HR_AVG(66,72,DatasetHeader::parseFloat),
    T_MAX(74,80,DatasetHeader::parseFloat),
    T_MIN(82,88,DatasetHeader::parseFloat),
    P_CALC(90,96,DatasetHeader::parseFloat),
    SOLARAD(98,103,DatasetHeader::parseInt),
    SOLARAD_FLAG(105,105,DatasetHeader::parseByte),
    SOLARAD_MAX(107,112,DatasetHeader::parseInt),
    SOLARAD_MAX_FLAG(114,114,DatasetHeader::parseByte),
    SOLARAD_MIN(116,121,DatasetHeader::parseInt),
    SOLARAD_MIN_FLAG(123,123,DatasetHeader::parseByte),
    SUR_TEMP_TYPE(125,125,null),
    SUR_TEMP(127,133,DatasetHeader::parseFloat),
    SUR_TEMP_FLAG(135,135,DatasetHeader::parseByte),
    SUR_TEMP_MAX(137,143,DatasetHeader::parseFloat),
    SUR_TEMP_MAX_FLAG(145,145,DatasetHeader::parseByte),
    SUR_TEMP_MIN(147,153,DatasetHeader::parseFloat),
    SUR_TEMP_MIN_FLAG(155,155,DatasetHeader::parseByte),
    RH_HR_AVG(157,161,DatasetHeader::parseInt),
    RH_HR_AVG_FLAG(163,163,DatasetHeader::parseByte),
    SOIL_MOISTURE_5(165,171,DatasetHeader::parseFloat),
    SOIL_MOISTURE_10(173,179,DatasetHeader::parseFloat),
    SOIL_MOISTURE_20(181,187,DatasetHeader::parseFloat),
    SOIL_MOISTURE_50(189,195,DatasetHeader::parseFloat),
    SOIL_MOISTURE_100(197,203,DatasetHeader::parseFloat),
    SOIL_TEMP_5(205,211,DatasetHeader::parseFloat),
    SOIL_TEMP_10(213,219,DatasetHeader::parseFloat),
    SOIL_TEMP_20(221,227,DatasetHeader::parseFloat),
    SOIL_TEMP_50(229,235,DatasetHeader::parseFloat),
    SOIL_TEMP_100(237,243,DatasetHeader::parseFloat);

    @FunctionalInterface
    interface Converter<T> { T convert(CharSequence sequence); }

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final Pattern MISSING_DATA_PATTERN     = Pattern.compile("-9+\\.0+$|-9+$");
    
    public final int idxColBegin, idxColEnd;
    private final Converter converter;

    private DatasetHeader(int idxColBegin, int idxColEnd, Converter converter)
    {
        this.idxColBegin = idxColBegin;
        this.idxColEnd   = idxColEnd;
        this.converter   = converter;
    }

    public int length()                            { return 1 + idxColEnd - idxColBegin; }
    public String getString(String string)         { return string.substring(idxColBegin - 1, idxColEnd); }
    public CharSequence getSequence(String string) { return string.subSequence(idxColBegin - 1, idxColEnd); }
    public boolean hasConverter()                  { return converter != null; }
    public Object convert(CharSequence sequence)   { return converter.convert(sequence); }
   
    public static boolean isMissing(CharSequence string)     { return MISSING_DATA_PATTERN.matcher(string).matches(); }
    private static int parseInt(CharSequence sequence)       { return (int) parseFloat(sequence); }
    private static float parseFloat(CharSequence sequence)   { return Float.parseFloat(sequence.toString()); }
    private static double parseDouble(CharSequence sequence) { return Double.parseDouble(sequence.toString()); }
    private static byte parseByte(CharSequence sequence)     { return Byte.valueOf(sequence.toString()); }
    private static int parseDate(CharSequence sequence)      { return LocalDate.parse(sequence, DATE_FORMATTER).getDayOfYear(); }
    private static int parseTime(CharSequence sequence)
    {
        int hour = Integer.parseInt(sequence, 0, 2, 10);
        int min  = Integer.parseInt(sequence, 2, 4, 10);
        return hour * 60 + min;
    }
}