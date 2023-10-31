package io.mycat.server.interceptor;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * jdbc执行结果返回拦截器
 */
public interface SQLResultInterceptor {

    /**
     * 待实现的业务接口
     */
    String intercept(String tableName, String fieldName, String fieldValue);
}
