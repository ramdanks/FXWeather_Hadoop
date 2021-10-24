package com.ramdan.fxweather;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

/** Specify the header and column index of a dataset.
 * not following the rules of zero-based indexing, this data was mapped from README.txt in the Datasets folder
 * @author Ramadhan Kalih Sewu
 * @version 0.1
*/
public enum Header
{
    WBANNO(1,5,Accumulate.DISCARD),
    UTC_DATE(7,14,Accumulate.DISCARD),
    UTC_TIME(16,19,Accumulate.DISCARD),
    LST_DATE(21,28,Accumulate.DISCARD),
    LST_TIME(30,33,Accumulate.DISCARD),
    CRX_VN(35,40,Accumulate.DISCARD),
    LONGITUDE(42,48,Accumulate.BEGIN),
    LATITUDE(50,56,Accumulate.BEGIN),
    T_CALC(58,64,Accumulate.AVG),
    T_HR_AVG(66,72,Accumulate.AVG),
    T_MAX(74,80,Accumulate.MAX),
    T_MIN(82,88,Accumulate.MIN),
    P_CALC(90,96,Accumulate.SUM),
    SOLARAD(98,103,Accumulate.AVG),
    SOLARAD_FLAG(105,105,Accumulate.FMAX),
    SOLARAD_MAX(107,112,Accumulate.MAX),
    SOLARAD_MAX_FLAG(114,114,Accumulate.FMAX),
    SOLARAD_MIN(116,121,Accumulate.MIN),
    SOLARAD_MIN_FLAG(123,123,Accumulate.FMAX),
    SUR_TEMP_TYPE(125,125,Accumulate.FMAX),
    SUR_TEMP(127,133,Accumulate.AVG),
    SUR_TEMP_FLAG(135,135,Accumulate.FMAX),
    SUR_TEMP_MAX(137,143,Accumulate.MAX),
    SUR_TEMP_MAX_FLAG(145,145,Accumulate.FMAX),
    SUR_TEMP_MIN(147,153,Accumulate.MIN),
    SUR_TEMP_MIN_FLAG(155,155,Accumulate.FMAX),
    RH_HR_AVG(157,161,Accumulate.AVG),
    RH_HR_AVG_FLAG(163,163,Accumulate.FMAX),
    SOIL_MOISTURE_5(165,171,Accumulate.AVG),
    SOIL_MOISTURE_10(173,179,Accumulate.AVG),
    SOIL_MOISTURE_20(181,187,Accumulate.AVG),
    SOIL_MOISTURE_50(189,195,Accumulate.AVG),
    SOIL_MOISTURE_100(197,203,Accumulate.AVG),
    SOIL_TEMP_5(205,211,Accumulate.AVG),
    SOIL_TEMP_10(213,219,Accumulate.AVG),
    SOIL_TEMP_20(221,227,Accumulate.AVG),
    SOIL_TEMP_50(229,235,Accumulate.AVG),
    SOIL_TEMP_100(237,243,Accumulate.AVG);

    @FunctionalInterface
    interface Converter<T> { T convert(CharSequence sequence); }

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final Pattern MISSING_DATA_PATTERN     = Pattern.compile("-9+\\.0+$|-9+$");

    public final int idxColBegin, idxColEnd;
    public final Accumulate accumulate;

    private Header(int idxColBegin, int idxColEnd, Accumulate accumulate)
    {
        this.idxColBegin = idxColBegin;
        this.idxColEnd   = idxColEnd;
        this.accumulate  = accumulate;
    }

    public int length()                            { return 1 + idxColEnd - idxColBegin; }
    public String getString(String string)         { return string.substring(idxColBegin - 1, idxColEnd); }
    public CharSequence getSequence(String string) { return string.subSequence(idxColBegin - 1, idxColEnd); }
    public boolean isMissing(String string)
    {
        if (string == null) return true;
        try
        {
            CharSequence sequence = getSequence(string);
            return MISSING_DATA_PATTERN.matcher(sequence).matches();
        }
        catch (Throwable ignored) { return true; }
    }

    public static int parseInt(CharSequence sequence)       { return (int) parseFloat(sequence); }
    public static float parseFloat(CharSequence sequence)   { return Float.parseFloat(sequence.toString()); }
    public static double parseDouble(CharSequence sequence) { return Double.parseDouble(sequence.toString()); }
    public static byte parseByte(CharSequence sequence)     { return Byte.valueOf(sequence.toString()); }
    public static int parseDate(CharSequence sequence)      { return LocalDate.parse(sequence, DATE_FORMATTER).getDayOfYear(); }
    public static int parseTime(CharSequence sequence)
    {
        int hour = Integer.parseInt(sequence.subSequence(0, 2).toString());
        int min  = Integer.parseInt(sequence.subSequence(2, 4).toString());
        return hour * 60 + min;
    }
}
