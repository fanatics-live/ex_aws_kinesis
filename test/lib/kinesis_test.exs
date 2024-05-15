defmodule ExAws.KinesisTest do
  use ExUnit.Case, async: true
  alias ExAws.Kinesis

  ## NOTE:
  # These tests are not intended to be operational examples, but intead mere
  # ensure that the form of the data to be sent to AWS is correct.
  #

  test "#put_records" do
    records = [
      %{data: "asdfasdfasdf", partition_key: "foo"},
      %{data: "foobar", partition_key: "bar", explicit_hash_key: "wuff"}
    ]

    assert Kinesis.put_records("logs", records).data ==
             %{
               "Records" => [
                 %{"Data" => "YXNkZmFzZGZhc2Rm", "PartitionKey" => "foo"},
                 %{"Data" => "Zm9vYmFy", "ExplicitHashKey" => "wuff", "PartitionKey" => "bar"}
               ],
               "StreamName" => "logs"
             }
  end

  test "#get_shard_iterator" do
    assert Kinesis.get_shard_iterator("logs", "shard-0000", :after_sequence_number).data ==
             %{
               "ShardId" => "shard-0000",
               "ShardIteratorType" => "AFTER_SEQUENCE_NUMBER",
               "StreamName" => "logs"
             }
  end
end
