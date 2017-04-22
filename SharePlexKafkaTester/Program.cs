using System;
using System.Collections.Generic;
using System.Data.OracleClient;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Converters;
using RdKafka;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;


namespace SharePlexKafkaTester
{
    class Program
    {
        //Topics list to save all topics from broker node
        public List<String> Topics = new List<string>();
        //list all activated broker node
        public List<String> Brokernodes = new List<String>();

        static void Main(string[] args)
        {
            var config = new Config(){ GroupId = "simple-csharp-consumer" };
            
            //maxium reads mssages
            long maxReads = long.Parse("3");

            //the partition number,default start 0
            int partitionposition = int.Parse("0");

            

            

            int Port = int.Parse("9092");



        }

        private void ReadTopicToList(List<String> brokernodes)
        {
            if (brokernodes == null)
            {
                System.Console.WriteLine("Cannot find out valid broker nodes in there, Please check your kfaka culters environment!");
                return;
            }


            foreach (string brokernode in brokernodes)
            {
                BrokerMetadata bmd = new BrokerMetadata();
                PartitionMetadata pm = new PartitionMetadata();
                bmd.Host = brokernode;
                bmd.Port = int.Parse("9092");

                var brokerconfig = new Config();
                brokerconfig.GroupId = "leaderLookup";
                
                EventConsumer brokerconsumer = new EventConsumer(brokerconfig,brokernode);

                
            }
            
        }

        private void FindBrokerNodeLeader(string brokernode, string port, int partition,string topic)
        {
            var nodeconfig = new Config();
            nodeconfig.GroupId = "leaderLookup";
            try 
            {
                EventConsumer nodeconsumer = new EventConsumer(nodeconfig, brokernode);
                TopicMetadata nodetopicmetadata = new TopicMetadata();
                nodetopicmetadata.Topic = topic;
                nodetopicmetadata.Partitions = nodetopicmetadata.Partitions;

                
                
            }
            catch
            {

            }
            finally
            {

            }


        }
    }
}
