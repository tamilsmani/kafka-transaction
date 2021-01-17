package com.example.event;

import org.springframework.context.ApplicationEvent;

public class CustomEvent extends ApplicationEvent {
	
	String msg;
	public CustomEvent(Object source, String msg) {
		super(source);
		this.msg  = msg;
		
	}
	public String getMsg() {
		return msg;
	}
	
	
}