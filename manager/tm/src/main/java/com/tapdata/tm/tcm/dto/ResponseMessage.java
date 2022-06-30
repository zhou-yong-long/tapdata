package com.tapdata.tm.tcm.dto;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.UUID;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 9:24 上午
 * @description
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class ResponseMessage<T>{

	public static final String OK = "ok";

	private String reqId = UUID.randomUUID().toString();

	/**
	 * 请求处理的代码
	 */
	protected String code = OK;

	/**
	 * 请求处理失败时的错误消息
	 */
	protected String message;

	/**
	 * 请求处理成功的数据
	 */
	protected T data;

}
