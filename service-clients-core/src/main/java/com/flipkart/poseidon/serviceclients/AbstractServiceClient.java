/*
 * Copyright 2015 Flipkart Internet, pvt ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.poseidon.serviceclients;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.phantom.task.impl.TaskContextFactory;
import com.flipkart.phantom.task.impl.TaskContextImpl;
import com.flipkart.phantom.task.impl.TaskHandler;
import com.flipkart.phantom.task.impl.TaskHandlerExecutorRepository;
import com.flipkart.phantom.task.impl.registry.TaskHandlerRegistry;
import com.flipkart.phantom.task.spi.TaskContext;
import com.flipkart.phantom.task.spi.TaskResult;
import com.flipkart.poseidon.handlers.http.impl.SinglePoolHttpTaskHandler;
import com.flipkart.poseidon.model.VariableModel;
import flipkart.lego.api.entities.ServiceClient;
import flipkart.lego.api.exceptions.LegoServiceException;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.flipkart.poseidon.handlers.http.HandlerConstants.*;
import static com.flipkart.poseidon.serviceclients.ServiceClientConstants.HEADERS;

/**
 * Created by mohan.pandian on 24/02/15.
 *
 * Generated service client implementations will extend this abstract class
 */
public abstract class AbstractServiceClient<R extends TaskHandler> implements ServiceClient {

    private Map<String, VertxHttpClient> vertxHttpClientMap = new HashMap<>();

    //Static Strings
    protected static final String GET = "GET";
    protected static final String POST = "POST";
    protected static final String PUT = "PUT";
    protected static final String PATCH = "PUT";
    protected static final String DELETE = "DELETE";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public AbstractServiceClient() {
        initClient(getCommandName());
    }

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    protected Map<String, Class<? extends ServiceClientException>> exceptions = new HashMap<>();

    protected abstract String getCommandName();

    protected final <T> FutureTaskResultToDomainObjectPromiseWrapper<T> execute(JavaType javaType, String uri, String httpMethod, Map<String, String> headersMap, Object requestObject) throws IOException {
        return execute(new ServiceExecutePropertiesBuilder().setJavaType(javaType).setUri(uri).setHttpMethod(httpMethod).setHeadersMap(headersMap).setRequestObject(requestObject).setRequestCachingEnabled(false).build());
    }

    protected final <T> FutureTaskResultToDomainObjectPromiseWrapper<T> execute(JavaType javaType, String uri, String httpMethod,
                                  Map<String, String> headersMap, Object requestObject, String commandName) throws IOException {
        return execute(new ServiceExecutePropertiesBuilder().setJavaType(javaType).setUri(uri).setHttpMethod(httpMethod).setHeadersMap(headersMap).setRequestObject(requestObject).setCommandName(commandName).setRequestCachingEnabled(false).build());
    }

    protected final <T> FutureTaskResultToDomainObjectPromiseWrapper<T> execute(JavaType javaType, String uri, String httpMethod, Map<String, String> headersMap, Object requestObject, String commandName, boolean requestCachingEnabled) throws IOException {
        return execute(new ServiceExecutePropertiesBuilder().setJavaType(javaType).setUri(uri).setHttpMethod(httpMethod).setHeadersMap(headersMap).setRequestObject(requestObject).setCommandName(commandName).setRequestCachingEnabled(requestCachingEnabled).build());
    }

    protected final <T> FutureTaskResultToDomainObjectPromiseWrapper<T> execute(ServiceExecuteProperties properties) throws IOException {
        Logger logger = LoggerFactory.getLogger(getClass());

        String commandName = properties.getCommandName();
        String uri = properties.getUri();
        String httpMethod = properties.getHttpMethod();
        boolean requestCachingEnabled = properties.isRequestCachingEnabled();
        Map<String, String> headersMap = properties.getHeadersMap();
        Object requestObject = properties.getRequestObject();
        JavaType javaType = properties.getJavaType();
        JavaType errorType = properties.getErrorType();
        Map<String, ServiceResponseInfo> serviceResponseInfoMap = properties.getServiceResponseInfoMap();

        if (commandName == null || commandName.isEmpty()) {
            commandName = getCommandName();
        }

        if (ServiceContext.get(ServiceClientConstants.COLLECT_COMMANDS) != null && (boolean) ServiceContext.get(ServiceClientConstants.COLLECT_COMMANDS)) {
            ConcurrentLinkedQueue<String> commandNames = ServiceContext.get(ServiceClientConstants.COMMANDS);
            commandNames.add(commandName);
        }

        logger.info("Executing {} with {} {}", commandName, httpMethod, uri);

        Map<String, Object> params = new HashMap<>();
        params.put(HTTP_URI, uri);
        params.put(HTTP_METHOD, httpMethod);
        if (requestCachingEnabled) {
            params.put(X_CACHE_REQUEST, "true");
        }

        Map<String, String> injectedHeadersMap = injectHeaders(headersMap);
        if (!injectedHeadersMap.isEmpty()) {
            try {
                params.put(HTTP_HEADERS, injectedHeadersMap);
            } catch (Exception e) {
                logger.error("Error serializing headers", e);
                throw new IOException("Headers serialization error", e);
            }
        }
        boolean enableVertx = injectedHeadersMap.containsKey("enable-vertx");

        byte[] payload = null;
        if (requestObject != null) {
            try {
                if (requestObject instanceof String) {
                    payload = ((String) requestObject).getBytes();
                } else if (requestObject instanceof byte[]) {
                    payload = (byte[]) requestObject;
                } else {
                    payload = getObjectMapper().writeValueAsBytes(requestObject);
                }
            } catch (Exception e) {
                logger.error("Error serializing request object", e);
                throw new IOException("Request object serialization error", e);
            }
        }

        TaskContext taskContext = TaskContextFactory.getTaskContext();

        if (serviceResponseInfoMap.isEmpty()) {
            serviceResponseInfoMap.put("200", new ServiceResponseInfo(javaType, null));
            exceptions.forEach((status, errorClass) -> {
                serviceResponseInfoMap.put(status, new ServiceResponseInfo(errorType, errorClass));
            });
        }

        if (vertxHttpClientMap.get(commandName) == null) {
            initClient(commandName);
        }

        ServiceResponseDecoder<T> serviceResponseDecoder =
                new ServiceResponseDecoder<>(
                        getObjectMapper(), logger,
                        serviceResponseInfoMap, ServiceContext.getCollectedHeaders());

        final Boolean throwOriginal = ServiceContext.get(ServiceClientConstants.THROW_ORIGINAL);
        FutureTaskResultToDomainObjectPromiseWrapper<T> futureWrapper;
        if (vertxHttpClientMap.get(commandName).isClientCreated() && enableVertx) {
            futureWrapper = getVertxFutureWrapper(commandName, injectedHeadersMap, properties, serviceResponseDecoder, javaType);
        } else {
            Future<TaskResult> future = taskContext.executeAsyncCommand(commandName, payload, params, serviceResponseDecoder);
            futureWrapper = new FutureTaskResultToDomainObjectPromiseWrapper<>(future, Optional.ofNullable(throwOriginal).orElse(false));
        }
        if (ServiceContext.isDebug()) {
            properties.setHeadersMap(injectedHeadersMap);
            ServiceContext.addDebugResponse(this.getClass().getName(), new ServiceDebug(properties, futureWrapper));
        }

        return futureWrapper;
    }

    private <T> FutureTaskResultToDomainObjectPromiseWrapper<T> getVertxFutureWrapper(String commandName, Map<String, String> injectedHeadersMap
            , ServiceExecuteProperties properties, ServiceResponseDecoder<T> serviceResponseDecoder, JavaType javaType) {
        VertxHttpClient vertxHttpClient = vertxHttpClientMap.get(commandName);
        CompletableFuture<TaskResult> result = new CompletableFuture<>();
        FutureTaskResultToDomainObjectPromiseWrapper<T> futureWrapper = new FutureTaskResultToDomainObjectPromiseWrapper<>(result, getObjectMapper(), javaType);
        vertxHttpClient.getCircuitBreaker().execute(future -> {
            if (GET.equals(properties.getHttpMethod())) {
                HttpRequest<Buffer> request = vertxHttpClient.getClient().get(vertxHttpClient.getPort(), vertxHttpClient.getHost(), properties.getUri());
                addHeaders(request.headers(), injectedHeadersMap);
                request.send(ar->responseHandler(ar, future, result, serviceResponseDecoder));
            } else if (POST.equals(properties.getHttpMethod())) {
                HttpRequest<Buffer> request = vertxHttpClient.getClient().post(vertxHttpClient.getPort(), vertxHttpClient.getHost(), properties.getUri());
                addHeaders(request.headers(), injectedHeadersMap);
                request.sendJson(properties.getRequestObject(), ar->responseHandler(ar, future, result, serviceResponseDecoder));
            }
        }).setHandler(ar -> {
            if (ar.failed()) {
                result.completeExceptionally(ar.cause());
            }
        });
        return futureWrapper;
    }

    private <T> void responseHandler(AsyncResult<HttpResponse<Buffer>> ar, io.vertx.core.Future breakerFuture, CompletableFuture<TaskResult> result, ServiceResponseDecoder<T> serviceResponseDecoder) {
        if (ar.succeeded()) {
            serviceResponseDecoder.decodeAndComplete(ar.result(), breakerFuture, result);
        } else {
            result.completeExceptionally(ar.cause());
            breakerFuture.fail(ar.cause());
        }
    }

    private void addHeaders(MultiMap headers, Map<String, String> map) {
        if (map != null) {
            map.entrySet().stream().filter(entry -> entry.getKey() != null && entry.getValue() != null).forEach(entry -> {
                headers.set(entry.getKey(), entry.getValue());
            });
        }
    }

    private synchronized void initClient(String commandName) {
        if (TaskContextFactory.getTaskContext() == null || vertxHttpClientMap.get(commandName) != null) {
            return;
        }
        TaskHandlerExecutorRepository taskHandlerExecutorRepository = ((TaskContextImpl) TaskContextFactory.getTaskContext()).getExecutorRepository();
        TaskHandler baseTaskHandler = ((TaskHandlerRegistry)taskHandlerExecutorRepository.getRegistry()).getTaskHandlerByCommand(commandName);
        if (baseTaskHandler instanceof SinglePoolHttpTaskHandler) {
            SinglePoolHttpTaskHandler taskHandler = (SinglePoolHttpTaskHandler) baseTaskHandler;
            WebClientOptions webClientOptions = new WebClientOptions().setKeepAlive(true).setUserAgentEnabled(false)
                    .setMaxPoolSize(200)
                    .setConnectTimeout(taskHandler.getConnectionTimeout())
                    .setMetricsName(commandName).setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
                    .setTryUseCompression(taskHandler.isRequestCompressionEnabled())
                    .setIdleTimeout(taskHandler.getOperationTimeout() + 100);
            WebClient client = WebClient.create(VertxManager.getVertx(), webClientOptions);
            CircuitBreaker circuitBreaker = CircuitBreaker.create(commandName, VertxManager.getVertx(),
                    new CircuitBreakerOptions().setMaxFailures(20).setTimeout(taskHandler.getOperationTimeout()).setFallbackOnFailure(false).setResetTimeout(5000)
            );
            vertxHttpClientMap.put(commandName, new VertxHttpClient(client, circuitBreaker, taskHandler.getHost(), taskHandler.getPort()));
            System.out.println(commandName + " initialized");
        } else {
            vertxHttpClientMap.put(commandName, new VertxHttpClient());
            System.out.println(commandName + " Failed to initialize");
        }
    }

    /**
     * Injects request id, perf test headers from RequestContext
     *
     * @param headersMap Original headers map sent from service client
     * @return Injected headers map
     */
    protected Map<String, String> injectHeaders(Map<String, String> headersMap) {
        Map<String, String> injectedHeadersMap = new HashMap<>();
        if (headersMap != null && !headersMap.isEmpty()) {
            injectedHeadersMap.putAll(headersMap);
        }

        // Inject Configured Headers
        if (ServiceContext.get(HEADERS) instanceof Map) {
            Map<String, String> configuredHeaders = ServiceContext.get(HEADERS);
            injectedHeadersMap.putAll(configuredHeaders);
        }

        return injectedHeadersMap;
    }

    protected String encodeUrl(Object url) throws JsonProcessingException {
        if (url == null) {
            return "";
        }

        if (url instanceof String) {
            return encodeUrl((String) url);
        } else if (ClassUtils.isPrimitiveOrWrapper(url.getClass()) || url.getClass().isEnum()) {
            return String.valueOf(url);
        } else {
            return encodeUrl(objectMapper.writeValueAsString(url));
        }
    }

    protected String encodeUrl(String url) {
        if (url == null || url.isEmpty()) {
            return "";
        }

        try {
            // Reverting back to ~ is available in existing mobile-api cp-service-client URLHelper.java. Keeping it...
            return URLEncoder.encode(url, "UTF-8").replaceAll("%7E", "~");
        } catch (UnsupportedEncodingException e) {
            LoggerFactory.getLogger(getClass()).error("Exception while encoding URL: " + url, e);
            return url;
        }
    }

    protected String getOptURI(String paramName, Object paramValue) {
        if (paramValue == null || paramValue instanceof String && paramValue.toString().isEmpty()) {
            return "";
        } else {
            return paramName + "=" + paramValue;
        }
    }

    protected String getQueryURI(List<String> params) {
        StringBuilder queryURI = new StringBuilder();
        Boolean first = true;
        for(String param: params) {
            if(param == null || param.isEmpty()) continue;
            if(first) {
                queryURI.append("?");
                first = false;
            } else {
                queryURI.append("&");
            }
            queryURI.append(param);
        }
        return queryURI.toString();
    }

    /*
     * Multivalue params are repeated in query section of URI under same param name
     * Ex: key=value1&key=value2&key=value3
     */
    protected <T> String getMultiValueParamURI(String paramName, List<T> paramValues) {
        StringBuilder queryURI = new StringBuilder();
        if (paramValues != null && !paramValues.isEmpty()) {
            boolean first = true;
            for (T paramValue: paramValues) {
                if (paramValue == null) {
                    continue;
                }

                if (first) {
                    first = false;
                } else {
                    queryURI.append("&");
                }
                queryURI.append(paramName).append("=");
                if (paramValue instanceof String) {
                    queryURI.append(encodeUrl((String) paramValue));
                } else {
                    queryURI.append(paramValue);
                }
            }
        }
        return queryURI.toString();
    }

    public JavaType getJavaType(TypeReference typeReference) {
        return objectMapper.getTypeFactory().constructType(typeReference);
    }

    public JavaType getErrorType(TypeReference typeReference) {
        return objectMapper.getTypeFactory().constructType(typeReference);
    }

    public <T> JavaType getJavaType(Class<T> clazz) {
        return objectMapper.getTypeFactory().constructType(clazz);
    }

    public <T> JavaType getErrorType(Class<T> clazz) {
        return objectMapper.getTypeFactory().constructType(clazz);
    }

    public JavaType constructJavaType(VariableModel variableModel) {
        if (variableModel == null) {
            return null;
        }

        Class<?> clazz;
        try {
            clazz = Class.forName(variableModel.getType());
        } catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException("Specify a known class " + variableModel.getType(), e);
        }

        JavaType[] javaTypes = new JavaType[variableModel.getTypes().length];
        for (int i = 0; i < variableModel.getTypes().length; i++) {
            javaTypes[i] = constructJavaType(variableModel.getTypes()[i]);
        }

        return objectMapper.getTypeFactory().constructParametrizedType(clazz, clazz, javaTypes);
    }

    @Override
    public void init() throws LegoServiceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutDown() throws LegoServiceException {
        throw new UnsupportedOperationException();
    }

    protected ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
