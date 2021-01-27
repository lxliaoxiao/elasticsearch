package com.elasticsearch.tools;

import java.awt.AWTException;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.elasticsearch.bean.UserInfo;

public class SourceUtils {
 
    public static List<UserInfo> file2list(String fileName) throws IOException {
        File file = new File(fileName);
        List<String> lines = FileUtils.readLines(file,"UTF-8");
        List<UserInfo> products = new ArrayList<UserInfo>();
        for (String line : lines) {
        	UserInfo p = line2bean(line);
            products.add(p);
        }
        return products;
    }
     
    private static UserInfo line2bean(String line) {
    	UserInfo userInfo = new UserInfo();
        String[] fields = line.split(",");
        userInfo.setName(fields[0]);
        userInfo.setAddress(fields[1]);
        userInfo.setRemark(fields[2]);
        userInfo.setAge(Integer.parseInt(fields[3]));
        userInfo.setSalary(Float.parseFloat(fields[4]));
        userInfo.setBirthDate(fields[5]);
        
        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS" );
        Date date = null;
        try {
        	date = sdf.parse(fields[6]);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        userInfo.setCreateTime(date);
        return userInfo;
    }
}
