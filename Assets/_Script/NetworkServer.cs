using UnityEngine;
// using UnityEngine.Assertions;
using Unity.Collections;
using Unity.Networking.Transport;
using System.Text; //for encoding

public class NetworkServer : MonoBehaviour
{
    //NetworkDriver = wrapper class (with extra features) of a socket
    //owns the socket, sends and receives packets, and keeps track of connections
    private NetworkDriver networkDriver;
    //each connection is a handle pointing to one client
    private NativeList<NetworkConnection> networkConnections;
    NetworkPipeline reliableAndInOrderPipeline;
    NetworkPipeline nonReliableNotInOrderedPipeline;

    const ushort NetworkPort = 9001;
    const int MaxNumClientConnections = 1000;

    void Start()
    {
        //Allocator.Persistent = needs manual disposal in memory
        networkConnections = new(MaxNumClientConnections, Allocator.Persistent);

        networkDriver = NetworkDriver.Create();
        reliableAndInOrderPipeline = networkDriver.CreatePipeline(typeof(FragmentationPipelineStage), typeof(ReliableSequencedPipelineStage));
        nonReliableNotInOrderedPipeline = networkDriver.CreatePipeline(typeof(FragmentationPipelineStage));
        //NetworkEndpoint = a network target address and port pair
        //AnyIpv4 = any IPv4 address on this machine (0.0.0.0)
        NetworkEndpoint endpoint = NetworkEndpoint.AnyIpv4;
        endpoint.Port = NetworkPort;

        int error = networkDriver.Bind(endpoint);
        if (error != 0)
            Debug.Log("Failed to bind to port " + NetworkPort);
        else
            networkDriver.Listen();
    }

    void OnDestroy()
    {
        if (networkDriver.IsCreated) networkDriver.Dispose();
        if (networkConnections.IsCreated) networkConnections.Dispose();
    }

    void Update()
    {
        #region Check Input and Send Msg

        if (Input.GetKeyDown(KeyCode.A))
        {
            for (int i = 0; i < networkConnections.Length; i++)
            {
                SendMessageToClient("Hello client's world, sincerely your network server", networkConnections[i]);
            }
        }

        #endregion

        networkDriver.ScheduleUpdate().Complete();

        #region Remove Unused Connections

        for (int i = 0; i < networkConnections.Length; i++)
        {
            if (!networkConnections[i].IsCreated)
            {
                //RemoveAtSwapBack(i) deletes item i quickly
                //by moving the last item into its place
                //instead of shifting everything down
                networkConnections.RemoveAtSwapBack(i);
                i--;
            }
        }

        #endregion

        #region Accept New Connections

        while (AcceptIncomingConnection())
        {
            Debug.Log("Accepted a client connection");
        }

        #endregion

        #region Manage Network Events

        for (int i = 0; i < networkConnections.Length; i++)
        {
            if (!networkConnections[i].IsCreated)
                continue;

            while (PopNetworkEventAndCheckForData(networkConnections[i], out var networkEventType, out var streamReader, out var pipelineUsedToSendEvent))
            {
                if (pipelineUsedToSendEvent == reliableAndInOrderPipeline)
                    Debug.Log("Network event from: reliableAndInOrderPipeline");
                else if (pipelineUsedToSendEvent == nonReliableNotInOrderedPipeline)
                    Debug.Log("Network event from: nonReliableNotInOrderedPipeline");

                switch (networkEventType)
                {
                    case NetworkEvent.Type.Data:
                        int sizeOfDataBuffer = streamReader.ReadInt();
                        if (sizeOfDataBuffer < 0 || sizeOfDataBuffer > streamReader.Length - streamReader.GetBytesRead())
                        {
                            Debug.LogWarning("Bad message size");
                            break;
                        }
                        byte[] byteBuffer = new byte[sizeOfDataBuffer];
                        streamReader.ReadBytes(byteBuffer);
                        string msg = Encoding.Unicode.GetString(byteBuffer);
                        ProcessReceivedMsg(msg, networkConnections[i]);
                        break;
                    case NetworkEvent.Type.Disconnect:
                        Debug.Log("Client has disconnected from server:" + (Unity.Networking.Transport.Error.DisconnectReason)streamReader.ReadByte());
                        networkConnections[i] = default;
                        break;
                }
            }
        }

        #endregion
    }

    private bool AcceptIncomingConnection()
    {
        //returns the next client waiting to connect
        NetworkConnection connection = networkDriver.Accept();
        if (connection == default) return false; //if nobody is waiting

        networkConnections.Add(connection);
        return true;
    }

    private bool PopNetworkEventAndCheckForData(NetworkConnection networkConnection, out NetworkEvent.Type networkEventType, out DataStreamReader streamReader, out NetworkPipeline pipelineUsedToSendEvent)
    {
        networkEventType = networkConnection.PopEvent(networkDriver, out streamReader, out pipelineUsedToSendEvent);

        if (networkEventType == NetworkEvent.Type.Empty)
            return false;
        return true;
    }

    private void ProcessReceivedMsg(string msg, NetworkConnection sender)
    {
        Debug.Log("Msg received = " + msg);
    }

    public void SendMessageToClient(string msg, NetworkConnection networkConnection)
    {
        if (!networkConnection.IsCreated) return;

        int status = networkDriver.BeginSend(reliableAndInOrderPipeline, networkConnection, out var streamWriter);
        if (status != 0)
        {
            Debug.Log("BeginSend failed: " + status);
            return;
        }

        byte[] msgAsByteArray = Encoding.Unicode.GetBytes(msg);
        streamWriter.WriteInt(msgAsByteArray.Length);
        streamWriter.WriteBytes(msgAsByteArray);
        int sent = networkDriver.EndSend(streamWriter);
        if (sent < 0)
            Debug.LogWarning("EndSend failed: " + sent);
    }
}