package ut.distcomp.framework;

import java.util.*;

//TODO arrange these according to the clients which handle them: Env, nodes, clients

public class BayouMessage {
	ProcessId src;
}

class endOfAntiEntropy extends BayouMessage{
	public endOfAntiEntropy(ProcessId src){
		this.src=src;
	}
}
class ACK extends BayouMessage{
	public ACK(ProcessId src){
		this.src=src;
	}
}
class YouArePrimaryMessage extends BayouMessage{
	YouArePrimaryMessage(ProcessId src){
		this.src = src;
	}
}
class UpdateMessage extends BayouMessage{
	String updateStr;
	int wid;
	int client_id;
	UpdateMessage(ProcessId src, String updateStr, int wid, int client_id){
		this.src = src; this.updateStr = updateStr; this.wid=wid; this.client_id = client_id;
	}
}
class ClientUpdateMessage extends BayouMessage{
	String updateStr;
	ProcessId dst;
	ClientUpdateMessage(ProcessId src, String updateStr, ProcessId dst){
		this.src = src; this.updateStr = updateStr; this.dst=dst;
	}
}
class QueryMessage extends BayouMessage{
	String songName; int wid; int client_id;
	QueryMessage(ProcessId src, String songName, int wid, int client_id){
		this.src = src; this.songName = songName;this.wid=wid; this.client_id = client_id;
	}
} 
class ClientQueryMessage extends BayouMessage{
	String songName; ProcessId dst;
	ClientQueryMessage(ProcessId src, String songName, ProcessId dst){
		this.src = src; this.songName = songName; this.dst=dst;
	}
}
class PrintLogMessage extends BayouMessage{
	PrintLogMessage(ProcessId src){
		this.src = src;
	}
}
class askAntiEntropyInfo extends BayouMessage{
	askAntiEntropyInfo(ProcessId src) {
		this.src=src;
	}
}
class AntiEntropyInfoMessage extends BayouMessage{
	HashMap<String, Integer> versionVector; int CSN; String server_id;
	AntiEntropyInfoMessage(ProcessId src,HashMap<String, Integer> version_vector,int csn, String server_id) {
		this.src=src; this.versionVector = version_vector; this.CSN=csn; this.server_id=server_id; 
	}
}
class CommitNotification extends BayouMessage{
	int accept_stamp;String serverID; int CSN;
	CommitNotification(ProcessId src,int accept_stamp,String serverID,int csn) {
		this.src=src;this.accept_stamp=accept_stamp;this.serverID=serverID;this.CSN=csn;
	}
	public String toString(){
		return "CommitNotification("+accept_stamp+","+serverID+","+CSN+")";
	}
}
class WriteMessage extends BayouMessage{
	Write w;
	WriteMessage(ProcessId src, Write w){
		this.src=src;this.w=w;
	}
	public String toString(){
		return "WriteMessage("+w+")";
	}
}
class FailureToSendMessage extends BayouMessage{
	ProcessId dst;
	public FailureToSendMessage(ProcessId src, ProcessId dst){
		this.src = src; this.dst = dst;
	}
}
class RetireMessage extends BayouMessage{
	RetireMessage(ProcessId src){
		this.src = src;
	}
}
class ResponseMessage extends BayouMessage{
	String response;
	ResponseMessage(ProcessId src, String response){
		this.src = src; this.response = response;
	}
}
class CreationMessage extends BayouMessage{
	public CreationMessage(ProcessId src){
		this.src=src;
	}
}
class ServerIDMessage extends BayouMessage{
	String server_id; String parent_id;
	ServerIDMessage(ProcessId src, String server_id, String parent_id){
		this.src = src;
		this.server_id = server_id;
		this.parent_id = parent_id;
	}
} 
class WIDQuery extends BayouMessage{
	int client_id;
	public WIDQuery(ProcessId src,int client_id) {
		this.client_id=client_id;
		this.src=src;
	}
}
class WIDMsg extends BayouMessage{
	int WID;
	public WIDMsg(ProcessId src,int WID) {
		this.src=src;
		this.WID=WID;
	}
}