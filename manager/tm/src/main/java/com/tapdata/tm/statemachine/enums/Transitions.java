/**
 * @title: State
 * @description:
 * @author lk
 * @date 2021/11/15
 */
package com.tapdata.tm.statemachine.enums;

public enum Transitions {

	/**
	 * source: edit/stopped/error
	 * target: scheduled
	 **/
	DATAFLOW_START(DataFlowEvent.START, new DataFlowState[]{DataFlowState.EDIT,DataFlowState.STOPPED,DataFlowState.ERROR}),
	/**
	 * source: running
	 * target: scheduling
	 **/
	DATAFLOW_OVERTIME(DataFlowEvent.OVERTIME, new DataFlowState[]{DataFlowState.RUNNING}),

	/**
	 * source: edit/stopped/error
	 * target: scheduling
	 **/
	SUBTASK_START(DataFlowEvent.START, new SubTaskState[]{SubTaskState.EDIT,SubTaskState.STOPPED,SubTaskState.ERROR, SubTaskState.DONE, SubTaskState.SCHEDULING_FAILED}),
	/**
	 * source: scheduling
	 * target: wait_run
	 **/
	SUBTASK_SCHEDULE_SUCEESS(DataFlowEvent.SCHEDULE_SUCCESS, new SubTaskState[]{SubTaskState.SCHEDULING}),
	/**
	 * source: wait_run
	 * target: scheduling
	 **/
	SUBTASK_OVERTIME(DataFlowEvent.OVERTIME, new SubTaskState[]{SubTaskState.WAITING_RUN}),
	;

	private Enum event;
	
	private Enum[] sources;

	Transitions(Enum event, Enum[] sources) {
		this.event = event;
		this.sources = sources;
	}

	public Enum getEvent() {
		return event;
	}

	public Enum[] getSources() {
		return sources;
	}
}
