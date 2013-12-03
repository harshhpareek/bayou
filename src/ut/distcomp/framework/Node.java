package ut.distcomp.framework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Node extends Process{
	int node_id;
	String server_id;
	int CSN=0;
	int old_CSN=-1;
	int accept_stamp=0;
	HashMap<String,Integer> version_vector = new HashMap<String,Integer>(); 
	HashMap<String,Integer> old_version_vector = null;
	Set<Write> tentativeWrites = new TreeSet<Write>();
	Set<Write> committedWrites = new TreeSet<Write>();
	Boolean exitFlag = false;
	Boolean exitOnNextAntientropy = false;
	Boolean isPrimary;
	Boolean acceptingClientRequests=true;
	PlayList db;

	final int inf = 99999;

	public Node(Env env, ProcessId me, int nodeId, Boolean isPrimary){
		this.node_id=nodeId;
		this.me = me;
		this.env = env;
		this.isPrimary = isPrimary;
		this.db = new PlayList();
		env.addProc(me, this);
	}

	public void add_entry(String command, int wid, int client_id){
		if(isPrimary){
			CSN++; accept_stamp++;
			committedWrites.add(new Write(server_id, accept_stamp, CSN, command, wid, client_id));
			if(command.split(";")[0].equals("creation") || command.split(";")[0].equals("retire"))
				;
			else
				db.execute(command);
		}
		else{
			accept_stamp++;
			tentativeWrites.add(new Write(server_id, accept_stamp, -1, command, wid, client_id));
		}
		version_vector.put(server_id,accept_stamp);
	}

	int CompleteV(HashMap<String,Integer> r_versionVector, String w_serverID){
		if(r_versionVector.containsKey(w_serverID)){
			return r_versionVector.get(w_serverID);
		}
		else if(w_serverID.equals("0:primary")) //first server
			return inf;
		else{
			String[] s =w_serverID.split(":",2);
			int tki = Integer.parseInt(s[0]);
			if(CompleteV(r_versionVector, s[1])>=tki+1)
				return inf;
			else
				return -inf;
		}
	}

	public void anti_entropy(ProcessId R, HashMap<String,Integer> r_versionVector, int r_csn, String r_server_id){
		if(r_csn<CSN){
			Iterator<Write> it = committedWrites.iterator(); 
			while(it.hasNext()){
				Write cw = it.next();
				if(cw.CSN>r_csn){
					int r_accept_stamp = CompleteV(r_versionVector,cw.serverID);
					if(cw.accept_stamp<=r_accept_stamp)
						sendMessage(R,new CommitNotification(me, cw.accept_stamp, cw.serverID, cw.CSN));
					else if(r_accept_stamp<inf){
						sendMessage(R, new WriteMessage(me, cw));
					}
				}
			}
		}
		Iterator<Write> it = tentativeWrites.iterator(); 
		while(it.hasNext()){
			Write tw = it.next();
			int r_accept_stamp = CompleteV(r_versionVector,tw.serverID);
			if(r_accept_stamp<tw.accept_stamp)
				sendMessage(R, new WriteMessage(me, tw));
		}

		if(exitOnNextAntientropy)
		{
			sendMessage(R, new endOfAntiEntropy(me));
			ArrayList<BayouMessage> pendingMessages = new ArrayList<BayouMessage>();
			long start = System.currentTimeMillis();
			while(System.currentTimeMillis() - start <5000L){
				BayouMessage msg1 = getNextMessage(5000L);
				if(msg1 instanceof ACK){
					if(isPrimary){
						System.err.println(msg1.src+" is the new primary");
						sendMessage(msg1.src, new YouArePrimaryMessage(me));
					}
					exitFlag = true;
					printLog();
					break;
				} else if(msg1==null){

				}
				else{
					pendingMessages.add(msg1);
				}
			}
			for(BayouMessage m2:pendingMessages){
				deliver(m2);
			}
		}
	}

	public void retire(){
		acceptingClientRequests=false;
		System.err.println(me+" got retirement command and no longer accepting client requests");
		add_entry("retire;"+me+";"+server_id, -1, -1);
		exitOnNextAntientropy=true;
	}

	public void printLog(){
		System.out.println(server_id+" accept_stamp= "+accept_stamp+" CSN="+CSN+"\nCommited Writelog for "+server_id+":"+committedWrites+"\n"+"Tentative Writelog for "+server_id+":"+tentativeWrites + " version_vector "+version_vector);
		System.out.flush();
	}

	@Override
	void body() {
		System.err.println("Here I am: " + me);

		if(isPrimary){
			//Entering an empty environment.
			server_id=accept_stamp+":"+"primary";
			add_entry("creation;"+me+";"+server_id, -1, -1);
		}
		else{
			boolean gotId=false;
			ArrayList<BayouMessage> pendingMessages = new ArrayList<BayouMessage>();
			while(!gotId){
				for(ProcessId nodeid: env.Nodes.nodes){
					if(nodeid!=null && nodeid!=me){
						sendMessage(nodeid, new CreationMessage(me));
						break;
					}
				}
				long start = System.currentTimeMillis();
				while(System.currentTimeMillis()-start<5000L){ //TODO decide timeout here
					BayouMessage m = getNextMessage(5000L);
					if(m instanceof ServerIDMessage){
						ServerIDMessage msg = (ServerIDMessage) m;
						server_id = msg.server_id;
						accept_stamp = Integer.parseInt(server_id.split(":")[0]);
						System.out.println(me+" was assigned server_id "+server_id);
						version_vector.put(server_id,accept_stamp);
						version_vector.put(msg.parent_id,-inf);
						gotId=true;
						sendMessage(msg.src,new ACK(me));
						break;
					}
					else
						pendingMessages.add(m);
				}
			}
			for(BayouMessage m: pendingMessages)
				deliver(m);
		}

		while(!exitFlag){
			//printLog();
			//delay(500);
			if(inbox.ll.isEmpty()){ 
				Boolean newInformation = true;
				if(old_version_vector==null){
					newInformation = true;
				} else if(!old_version_vector.equals(version_vector)){
					newInformation = true;
				} else if(CSN==old_CSN){
					newInformation = true;
				}

				if(newInformation){
					old_version_vector = (HashMap<String, Integer>) version_vector.clone();
					old_CSN = CSN;
					delay(2000);
					for(ProcessId nodeid: env.Nodes.nodes){
						sendMessage(nodeid, new askAntiEntropyInfo(me));
					}
				}
			}
			BayouMessage m = getNextMessage();
			if(exitOnNextAntientropy){
				if(m instanceof AntiEntropyInfoMessage){
					AntiEntropyInfoMessage msg = (AntiEntropyInfoMessage) m;
					anti_entropy(msg.src,msg.versionVector,msg.CSN, msg.server_id);
				}else if(m instanceof askAntiEntropyInfo){
					askAntiEntropyInfo msg = (askAntiEntropyInfo) m;
					if(msg.src!=me){
						sendMessage(msg.src, new AntiEntropyInfoMessage(me, version_vector, CSN,server_id));
					}
				}else if(m instanceof PrintLogMessage){
					printLog();
				}
			}
			else if(m instanceof AntiEntropyInfoMessage){
				AntiEntropyInfoMessage msg = (AntiEntropyInfoMessage) m;
				anti_entropy(msg.src,msg.versionVector,msg.CSN, msg.server_id);
			}
			else if(m instanceof askAntiEntropyInfo){
				askAntiEntropyInfo msg = (askAntiEntropyInfo) m;
				if(msg.src!=me){
					sendMessage(msg.src, new AntiEntropyInfoMessage(me, version_vector, CSN,server_id));
				}
			}
			else if(m instanceof RetireMessage){
				retire();
			}
			else if(m instanceof PrintLogMessage){
				printLog();
			}
			else if(m instanceof CreationMessage){
				CreationMessage msg = (CreationMessage) m;
				sendMessage(msg.src, new ServerIDMessage(me, accept_stamp+":"+server_id,server_id));
				String new_server_id = accept_stamp+":"+server_id;
				ArrayList<BayouMessage> pendingMessages = new ArrayList<BayouMessage>();
				long start = System.currentTimeMillis();
				while(System.currentTimeMillis() - start <5000L){
					BayouMessage msg1 = getNextMessage(5000L);
					if(msg1 instanceof ACK){
						version_vector.put(new_server_id,-inf);
						add_entry("creation;"+msg.src.name+";"+new_server_id, -1, -1);
						System.err.println(me+" created a node and my accept stamp is "+accept_stamp);
						System.out.println(me+" assigned server_id "+new_server_id+" to "+msg.src);
						sendParentState(msg.src);
						break;
					} else if(msg1==null){

					}
					else{
						pendingMessages.add(msg1);
					}
				}
				for(BayouMessage m2:pendingMessages){
					deliver(m2);
				}
			}
			else if(m instanceof UpdateMessage){
				UpdateMessage msg = (UpdateMessage) m;
				if(acceptingClientRequests){
					add_entry(msg.updateStr, msg.wid, msg.client_id);
				}
				else
					System.err.println("Not accepting client requests. Dropped: "+msg.updateStr);
			}
			else if(m instanceof QueryMessage){
				QueryMessage msg = (QueryMessage) m;
				if(acceptingClientRequests){
					sendMessage(msg.src, new ResponseMessage(me, db.query(msg.songName)));
				}
				else
					System.err.println("Not accepting client requests. Dropped: "+msg.songName);
			}
			else if(m instanceof CommitNotification){
				CommitNotification msg = (CommitNotification) m;
				commitTentativeWrite(msg.accept_stamp,msg.serverID,msg.CSN);
			}
			else if(m instanceof WriteMessage){
				WriteMessage msg = (WriteMessage) m;
				if(msg.w.nonRejectable){
					if(msg.w.CSN==-1){
						tentativeWrites.add(msg.w);
						version_vector.put(msg.w.serverID, msg.w.accept_stamp);
					}
					else
						commitNewWrite(msg.w);
					if(msg.w.command.split(";")[0].equals("creation")){
						String new_server_id = msg.w.command.split(";")[2];
						int accept_stamp = Integer.parseInt(new_server_id.split(":")[0]);
						if(!version_vector.containsKey(new_server_id))
							version_vector.put(new_server_id, -inf);
					}
					else if(msg.w.command.split(";")[0].equals("retire")){
						String retiring_server_id = msg.w.command.split(";")[2];
						version_vector.remove(retiring_server_id);
					}
				}
				else{
					if(!isPrimary){
						if(isNewMsg(msg)){
							if(version_vector.containsKey(msg.w.serverID)){
								if(msg.w.CSN==-1){
									tentativeWrites.add(msg.w);
									version_vector.put(msg.w.serverID, msg.w.accept_stamp);
								}
								else
									commitNewWrite(msg.w);
								if(msg.w.command.split(";")[0].equals("creation")){
									String new_server_id = msg.w.command.split(";")[2];
									int accept_stamp = Integer.parseInt(new_server_id.split(":")[0]);
									System.err.println(server_id + "creating "+new_server_id);
									if(!version_vector.containsKey(new_server_id))
										version_vector.put(new_server_id, -inf);
								}
								else if(msg.w.command.split(";")[0].equals("retire")){
									String retiring_server_id = msg.w.command.split(";")[2];
									version_vector.remove(retiring_server_id);
								}
							}
						}
					}
					else{
						if(isNewMsg(msg)){
							if(version_vector.containsKey(msg.w.serverID)){
								version_vector.put(msg.w.serverID, msg.w.accept_stamp);
								CSN++;
								committedWrites.add(new Write(msg.w.serverID,msg.w.accept_stamp,CSN,msg.w.command,msg.w.wid,msg.w.client_id));
								if(msg.w.command.split(";")[0].equals("creation")){
									String new_server_id = msg.w.command.split(";")[2];
									int accept_stamp = Integer.parseInt(new_server_id.split(":")[0]);
									if(!version_vector.containsKey(new_server_id))
										version_vector.put(new_server_id, -inf);
								}
								else if(msg.w.command.split(";")[0].equals("retire")){
									String retiring_server_id = msg.w.command.split(";")[2];
									version_vector.remove(retiring_server_id);
								}
							}
						}
					}
				}
			}
			else if(m instanceof YouArePrimaryMessage){
				isPrimary=true;
				commitAllTentativeWrites();
				System.err.println(node_id+" is the new Primary");
			}
			else if(m instanceof WIDQuery){
				WIDQuery msg = (WIDQuery) m;
				int max_wid=0;
				for(Write w: committedWrites){
					if(w.client_id==msg.client_id){
						if(w.wid>max_wid)
							max_wid = w.wid;
					}
				}
				sendMessage(msg.src, new WIDMsg(me, max_wid));
			}
			else if(m instanceof endOfAntiEntropy){
				sendMessage(m.src, new ACK(me));
			}
		}
		env.Nodes.remove(node_id);
		env.connections.isolate(node_id);
	}

	private void sendParentState(ProcessId src) {
		Iterator<Write> itc = committedWrites.iterator(); 
		while(itc.hasNext()){
			Write cw = itc.next();
			sendMessage(src, new WriteMessage(me, new Write(cw.serverID, cw.accept_stamp, cw.CSN, cw.command, cw.wid, cw.client_id, true)));
		}
		Iterator<Write> it = tentativeWrites.iterator(); 
		while(it.hasNext()){
			Write tw = it.next();
			sendMessage(src, new WriteMessage(me, new Write(tw.serverID, tw.accept_stamp, tw.CSN, tw.command, tw.wid, tw.client_id, true)));
		}

	}

	private boolean isNewMsg(WriteMessage msg) {
		Iterator<Write> it = tentativeWrites.iterator(); 
		while(it.hasNext()){
			Write tw = it.next();
			if(tw.accept_stamp==msg.w.accept_stamp){
				if(tw.serverID.equals(msg.w.serverID))
					return false;
			}
		}
		Iterator<Write> itc = committedWrites.iterator(); 
		while(itc.hasNext()){
			Write cw = itc.next();
			if(cw.accept_stamp==msg.w.accept_stamp){
				if(cw.serverID.equals(msg.w.serverID))
					return false;
			}
		}
		return true;
	}

	private void commitNewWrite(Write r_w){
		//printLog();
		if(CSN>=r_w.CSN)
			return;
		if(r_w.CSN!=CSN+1)
			;
		else{
			System.err.println(server_id+" commitNewWrite "+r_w);
			committedWrites.add(r_w);
			CSN=r_w.CSN;
			version_vector.put(r_w.serverID, r_w.accept_stamp);
			if(r_w.command.split(";")[0].equals("creation") || r_w.command.split(";")[0].equals("retire"))
				return;
			db.execute(r_w.command);
		}
	}

	private void commitTentativeWrite(int sw_accept_stamp, String sw_serverID, int sw_csn) {
		if(CSN>=sw_csn)
			return;
		Iterator<Write> it = tentativeWrites.iterator(); 
		while(it.hasNext()){
			Write tw = it.next();
			if(tw.accept_stamp==sw_accept_stamp){
				if(tw.serverID.equals(sw_serverID)){
					if(sw_csn!=CSN+1)
						;
					else{
						System.err.println(server_id +" Committing tentative write "+tw);
						CSN=sw_csn;
						committedWrites.add(new Write(sw_serverID,sw_accept_stamp,sw_csn,tw.command, tw.wid, tw.client_id));
						it.remove();
						System.err.println("Committed tentative write "+tw);
						if(tw.command.split(";")[0].equals("creation") || tw.command.split(";")[0].equals("retire"))
							return;
						db.execute(tw.command);
					}
				}
			}
		}
	}

	void commitAllTentativeWrites(){
		Iterator<Write> it = tentativeWrites.iterator(); 
		while(it.hasNext()){
			Write tw = it.next();
			CSN++;
			committedWrites.add(new Write(tw.serverID, tw.accept_stamp, CSN, tw.command, tw.wid, tw.client_id));
			it.remove();
		}
	}
}
