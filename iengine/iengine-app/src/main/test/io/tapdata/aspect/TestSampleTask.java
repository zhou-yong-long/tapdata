package io.tapdata.aspect;

import io.tapdata.aspect.task.AspectTaskSession;

import java.util.concurrent.atomic.LongAdder;

@AspectTaskSession(includeTypes = "TEST_TARGET")
public class TestSampleTask extends SampleTask {
	LongAdder onStartCounter = new LongAdder();
	LongAdder onStopCounter = new LongAdder();
	ProcessorNodeProcessAspect nodeProcessAspect;
	public TestSampleTask() {
		super();
	}
	@Override
	public void onStart() {
		//TaskStartAspect
		onStartCounter.increment();
	}

	@Override
	protected Void handleProcessorNodeProcess(ProcessorNodeProcessAspect nodeProcessAspect) {
		this.nodeProcessAspect = nodeProcessAspect;
		return null;
	}

	@Override
	public void onStop() {
		//TaskStartAspect
		onStopCounter.increment();
	}
}
