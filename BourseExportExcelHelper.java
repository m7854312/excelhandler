package com.le.jr.trade.amapi.interfaces.helper;


import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mengwei1
 */
public class BourseExportExcelHelper {
    private static Logger logger = LoggerFactory.getLogger(BourseExportExcelHelper.class);
    //wrap 内容操作的字符大小界限
    private static final int wrapCharSize = 50;
    //每个excel最大数据量
    private static final int excelMaxSize = 50000;

    private List<File> list = new ArrayList<>();  //存放返回文件

    private Map<String, String> headKeys;

    private String fileName;

    private AtomicInteger workerNum = new AtomicInteger(0); // 任务数

    private volatile int fileNum = 0; //文件生成数

    private CountDownLatch downLatch = new CountDownLatch(1);

    private List<Thread> threads = new ArrayList<>();

    private volatile boolean stop = false;

    private BlockingDeque<List> blockingDueue = new LinkedBlockingDeque<>();

    /**
     * @param headKeys excel头部，key对应List中对象的属性名
     * @param fileName 导出excel名称
     */
    public BourseExportExcelHelper(Map<String, String> headKeys, String fileName) {
        this.headKeys = headKeys;
        if (!fileName.contains(".xls") && !fileName.contains(".XLS")
                && !fileName.contains(".xlsx") && !fileName.contains(".XLSX")) {
            fileName += ".xls";
        }
        this.fileName = fileName;
    }


    public List<File> getExportFiles() throws InterruptedException {
        blockingInsertData(new ArrayList());  //  不能用 标志变量
        downLatch.await();
        return list.isEmpty() ? null : list;
    }

    public void blockingInsertData(List data) throws InterruptedException {
        blockingDueue.putLast(data);
    }

    /**
     * 导出excel，此接口适用于大数据量 分批写入excel
     */
    public void startExportExcel4Bource() {
        doExport();
    }

    public void interrupt() {
        stop = true;
        for (Thread s : threads) {
            s.interrupt();
        }
    }

    private File createFile() {
        if (fileNum == 1) {   //已经生成了1个文件，这是第2个文件
            fileName = fileName.replaceAll("(?<=.*)\\.(?=.*)", "-1.");
        }
        if (fileNum >= 2) {  // 至少已经生成了2个文件
            fileName = fileName.replaceAll("(?<=.*)\\d(?=\\..*)", String.valueOf(fileNum));
        }
        File file = new File(new File(System.getProperty("java.io.tmpdir")), fileName);
        fileNum++;
        return file;
    }

    private void doExport() {
        if (stop) {
            return;
        }


        Thread s = new Thread(new Runnable() {
            @Override
            public void run() {

                File file = createHSSFWorkbookWithBlockingQueue(headKeys);
                if (file != null && file.exists())
                    list.add(file);
                if (workerNum.decrementAndGet() == 0) {
                    downLatch.countDown();
                }
            }
        });
        s.start();
        workerNum.incrementAndGet();
        threads.add(s);
    }

    private File createHSSFWorkbookWithBlockingQueue(Map<String, String> headKeys) {
        File file = createFile();
        try (OutputStream os = new FileOutputStream(file)) {
            // 创建excel工作簿
            HSSFWorkbook wb = new HSSFWorkbook();
            // 创建第一个sheet（页），并命名
            HSSFSheet sheet = wb.createSheet("sheet1");

            CreationHelper createHelper = wb.getCreationHelper();
            Set<String> sets = headKeys.keySet();
            int len = sets.size();
            // 提取vo属性名
            String[] columns = new String[len];
            for (int i = 0; i < len; i++) {
                columns[i] = sets.toArray()[i].toString();
            }
            //头部，第一行
            HSSFRow row = sheet.createRow(0);
            int i = 0;
            //定义style
            HSSFCellStyle style = getColumnTopStyle(wb, wb.createCellStyle());
            // 定义表头
            for (Iterator<String> it = sets.iterator(); it.hasNext(); ) {
                String key = it.next();
                HSSFCell cell = row.createCell(i++);
                cell.setCellValue(createHelper.createRichTextString(headKeys.get(key)));
                cell.setCellStyle(style);
            }
            style = getStyle(wb, wb.createCellStyle());
            //设置每行每列的值
            List dataList;
            int dataCount = excelMaxSize + 1;  //加1，保证有 excelMaxSize条数据，标题行不算
            int rowNum = 1;   //行号记录器
            while (true) {
                dataList = blockingDueue.takeFirst();

                if (rowNum + dataList.size() > dataCount) {
                    List l = new ArrayList();
                    for (int size = dataList.size(), j = dataCount - rowNum; size > j; size--) {
                        l.add(dataList.remove(j));
                    }
                    if (!l.isEmpty()) {
                        blockingDueue.putFirst(l);
                    }
                    doExport();
                }
                if (dataList.isEmpty()) {
                    // 如果第一个文件是空的，必须保留以表明没有数据。
                    // 如果第二、三....个文件是空的，则说明上个文件正好装下所有数据，此时抛出异常，上层删除此空文件
                    if (rowNum == 1 && fileNum > 1) {
                        throw new RuntimeException(file.getName()+"没数据，删除");
                    }
                    break;
                }
                for (Object o : dataList) {
                    if (o == null) {
                        continue;
                    }
                    // Row 行,Cell 方格 , Row 和 Cell 都是从0开始计数的
                    // 创建一行，在页sheet上
                    HSSFRow row1 = sheet.createRow(rowNum);

                    for (int n = 0; n < len; n++) {
                        HSSFCell cell = row1.createCell(n, HSSFCell.CELL_TYPE_STRING);
                        cell.setCellValue(getValue(o, columns[n]));
                    }
                    rowNum++;
                }
            }
            //自动调整宽度，设置wrap
            autoAdapt(sheet, wb, len, style);
            wb.write(os);
        }  catch (Exception e) {
            file.delete();
            logger.error(fileName + "导出报错", e);
            return null;
        }
        return file;
    }


    /**
     * 获取对应的值
     *
     * @param o
     * @param fieldName
     * @return
     */
    private String getValue(Object o, String fieldName) {
        try {
            Class c = o.getClass();
            Field field = c.getDeclaredField(fieldName);
            field.setAccessible(true);
            Object o1 = field.get(o);
            return toStr(o1, field.getType());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:ss:mm");
    private DecimalFormat df = new DecimalFormat("0.00");

    private String toStr(Object o, Class clazz) {
        if (o == null) {
            if (isNum(clazz)) {
                if (isDecimals(clazz)) {
                    return "0.00";
                }
                return "0";
            }

            return "";
        }
        if (Date.class.isInstance(o)) {
            return sdf.format(o);
        }
        if (Number.class.isInstance(o)) {
            return df.format(o);
        }
        return o.toString();
    }

    private boolean isDecimals(Class clazz) {
        return BigDecimal.class.equals(clazz) || Float.TYPE.equals(clazz) || Float.class.equals(clazz)
                || Double.class.equals(clazz) || Double.TYPE.equals(clazz);
    }

    private boolean isNum(Class clazz) {
        if (Number.class.isAssignableFrom(clazz)) {
            return true;
        }
        if (clazz.isPrimitive()) {
            if (!(Boolean.TYPE.equals(clazz) || Character.TYPE.equals(clazz) || Void.TYPE.equals(clazz))) {
                return true;
            }
        }
        return false;
    }

    /*
    * 列数据信息单元格样式
    */
    private HSSFCellStyle getStyle(HSSFWorkbook workbook, HSSFCellStyle style) {
        // 设置字体
//        HSSFFont font = workbook.createFont();
        //设置字体大小
        //font.setFontHeightInPoints((short)10);
        //字体加粗
        //font.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
        //设置字体名字
//        font.setFontName("Courier New");
        //设置底边框;
        style.setBorderBottom(HSSFCellStyle.BORDER_THIN);
        //设置底边框颜色;
        style.setBottomBorderColor(HSSFColor.BLACK.index);
        //设置左边框;
        style.setBorderLeft(HSSFCellStyle.BORDER_THIN);
        //设置左边框颜色;
        style.setLeftBorderColor(HSSFColor.BLACK.index);
        //设置右边框;
        style.setBorderRight(HSSFCellStyle.BORDER_THIN);
        //设置右边框颜色;
        style.setRightBorderColor(HSSFColor.BLACK.index);
        //设置顶边框;
        style.setBorderTop(HSSFCellStyle.BORDER_THIN);
        //设置顶边框颜色;
        style.setTopBorderColor(HSSFColor.BLACK.index);
        //在样式用应用设置的字体;
//        style.setFont(font);
        //设置自动换行;
        style.setWrapText(false);
        //设置水平对齐的样式为居中对齐;
        style.setAlignment(HSSFCellStyle.ALIGN_CENTER);
        //设置垂直对齐的样式为居中对齐;
        style.setVerticalAlignment(HSSFCellStyle.VERTICAL_CENTER);
        return style;
    }

    private void autoAdapt(HSSFSheet sheet, HSSFWorkbook wb, int len, HSSFCellStyle style) {
        //让列宽随着导出的列长自动适应
        for (int colNum = 0; colNum < len; colNum++) {
            int columnWidth = sheet.getColumnWidth(colNum) / 256;
            for (int rowNum = 1; rowNum < sheet.getLastRowNum(); rowNum++) {
                HSSFRow currentRow;
                //当前行未被使用过
                if (sheet.getRow(rowNum) == null) {
                    currentRow = sheet.createRow(rowNum);
                } else {
                    currentRow = sheet.getRow(rowNum);
                }
                if (currentRow.getCell(colNum) != null) {
                    HSSFCell currentCell = currentRow.getCell(colNum);
                    if (currentCell.getCellType() == HSSFCell.CELL_TYPE_STRING) {
                        int length = currentCell.getStringCellValue().getBytes().length;
                        //长度大于wrapCharSize个字符的利用wrapText处理
                        if (length < wrapCharSize) {
                            if (columnWidth < length) {
                                columnWidth = length;
                            }
                        } else {
                            columnWidth = wrapCharSize;
                            //设置内容wrap
                            currentCell.setCellStyle(getWrapStyle(wb, style));
                        }
                    }
                }
            }
            if (colNum == 0) {
                sheet.setColumnWidth(colNum, (columnWidth + 4) * 256);
            } else {
                sheet.setColumnWidth(colNum, (columnWidth + 4) * 256);
            }
        }
    }

    /*
     * wrap单元格样式
     */
    private HSSFCellStyle getWrapStyle(HSSFWorkbook workbook, HSSFCellStyle style) {
        style.setWrapText(true);
        style.setBorderLeft(CellStyle.BORDER_THIN);
        style.setBorderRight(CellStyle.BORDER_THIN);
        style.setBorderTop(CellStyle.BORDER_THIN);
        style.setBorderBottom(CellStyle.BORDER_THIN);
        style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);

        return style;
    }

    /*
     * 列头单元格样式
     */
    private HSSFCellStyle getColumnTopStyle(HSSFWorkbook workbook, HSSFCellStyle style) {

        // 设置字体
        HSSFFont font = workbook.createFont();
        //设置字体大小
        font.setFontHeightInPoints((short) 11);
        //字体加粗
        font.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
        //设置字体名字
        font.setFontName("Courier New");
        //设置样式;
//        HSSFCellStyle style = workbook.createCellStyle();
        //设置底边框;
        style.setBorderBottom(HSSFCellStyle.BORDER_THIN);
        //设置底边框颜色;
        style.setBottomBorderColor(HSSFColor.BLACK.index);
        //设置左边框;
        style.setBorderLeft(HSSFCellStyle.BORDER_THIN);
        //设置左边框颜色;
        style.setLeftBorderColor(HSSFColor.BLACK.index);
        //设置右边框;
        style.setBorderRight(HSSFCellStyle.BORDER_THIN);
        //设置右边框颜色;
        style.setRightBorderColor(HSSFColor.BLACK.index);
        //设置顶边框;
        style.setBorderTop(HSSFCellStyle.BORDER_THIN);
        //设置顶边框颜色;
        style.setTopBorderColor(HSSFColor.BLACK.index);
        //在样式用应用设置的字体;
        style.setFont(font);
        //设置自动换行;
        style.setWrapText(false);
        //设置水平对齐的样式为居中对齐;
        style.setAlignment(HSSFCellStyle.ALIGN_CENTER);
        //设置垂直对齐的样式为居中对齐;
        style.setVerticalAlignment(HSSFCellStyle.VERTICAL_CENTER);

        return style;
    }

    /**
     * 递归删除目录下的所有文件及子目录下所有文件
     *
     * @param dir 将要删除的文件目录
     * @return boolean Returns "true" if all deletions were successful.
     * If a deletion fails, the method stops attempting to
     * delete and returns "false".
     */
    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            //递归删除目录中的子目录下
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
}
