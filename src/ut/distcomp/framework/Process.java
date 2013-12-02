package ut.distcomp.framework;

public abstract class Process extends Thread {
	ProcessId me;
	Queue<BayouMessage> inbox = new Queue<BayouMessage>();
	Env env;

	abstract void body();

	public void run(){
		body();
		env.removeProc(me);
	}

	BayouMessage getNextMessage(){
		return inbox.bdequeue();
	}

	BayouMessage getNextMessage(long timeout){
		if(inbox.ll.size()==0){
			try {
				Thread.sleep(0);
			} catch (InterruptedException e) {
				System.err.println("getNextMessage delay sleep interrupted");
				e.printStackTrace();
			}
		}
		return inbox.bdequeue(timeout);
	}


	void sendMessage(ProcessId dst, BayouMessage msg){		
		if(env.connections.get(me,dst)){
			if(msg instanceof CommitNotification || msg instanceof WriteMessage)
				System.out.println(me+" sending "+msg+" to "+dst);
			//System.out.println("inbox "+me+inbox.ll);
			env.sendMessage(dst, msg);
		}
		else{
			//TODO just dropping packets for now. Should deliver failure message?
			//deliver(new FailureToSendMessage(me,dst));
		}
	}

	void deliver(BayouMessage msg){
		inbox.enqueue(msg);
	}
	
	void delay(long timeout){
		try {
			Thread.sleep(timeout);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
