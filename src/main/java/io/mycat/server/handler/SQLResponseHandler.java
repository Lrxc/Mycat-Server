package io.mycat.server.handler;

import io.mycat.server.interceptor.SQLResultInterceptor;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * jdbc执行结果返回值处理
 */
public class SQLResponseHandler {
    private static final SQLResponseHandler INSTANCE = new SQLResponseHandler();

    public static final SQLResponseHandler getInstance() {
        return INSTANCE;
    }

    private SQLResultInterceptor callback;

    //注册监听
    public void addListener(SQLResultInterceptor callback) {
        this.callback = callback;
    }

    //发送
    public String intercept(String tableName, String fieldName, String fieldValue) {
        if (callback == null) {
            return fieldValue;
        }
        //getAllImplementationClasses(SQLResultInterceptor.class);

        try {
            String newValue = callback.intercept(tableName, fieldName, fieldValue);
            if (newValue == null) {
                newValue = fieldValue;
            }
            return newValue;
        } catch (Exception e) {
        }
        return fieldValue;
    }

    /**
     * 获取接口的接口的所有实现类
     */
    public static List<Class> getAllImplementationClasses(Class c) {
        List<Class> classes = new ArrayList<>();
        Class[] declaredClasses = c.getInterfaces();
        for (Class declaredClass : declaredClasses) {
            if (c.isAssignableFrom(declaredClass) && !Modifier.isAbstract(declaredClass.getModifiers())) {
                classes.add(declaredClass);
            }
        }
        return classes;
    }
}
