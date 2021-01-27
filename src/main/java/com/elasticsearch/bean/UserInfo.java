package com.elasticsearch.bean;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class UserInfo {

	private String name;
    private String address;
    private String remark;
    private int age;
    private float salary;
    private String birthDate;
    private Date createTime;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public float getSalary() {
        return salary;
    }

    public void setSalary(float salary) {
        this.salary = salary;
    }

    public String getBirthDate() {
        return birthDate;
    }

    public void setBirthDate(String birthDate) {
        this.birthDate = birthDate;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

	@Override
	public String toString() {
		return "UserInfo [name=" + name + ", address=" + address + ", remark=" + remark + ", age=" + age + ", salary="
				+ salary + ", birthDate=" + birthDate + ", createTime=" + createTime + "]";
	}
	
	public Map toMap() {
        HashMap<String,Object> map = new HashMap<String, Object>();
        map.put("name", name);
        map.put("address", address);
        map.put("remark", remark);
        map.put("age", age);
        map.put("salary", salary);
        map.put("birthDate", birthDate);
        map.put("createTime", createTime);
         
        return map;
    }
    
}
