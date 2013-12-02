package ut.distcomp.framework;

import java.util.Arrays;

public class NodeList{
	ProcessId[] nodes;
	Env env;
	int numNodes=0;
	
	public NodeList(int maxNodes){
		nodes = new ProcessId[maxNodes];
		for(int i=0;i<nodes.length;i++)
			nodes[i]=null;		
	}
	
	public void add(int i){
		nodes[i]=new ProcessId("node:"+i);
		numNodes++;
	}
	
	public void remove(int i){
		nodes[i]=null;
		numNodes--;
	}

	public ProcessId getProcessId(Integer i) {
		return nodes[i];
	}
	
	public boolean isEmpty(){
		return(numNodes==0);
	}
}