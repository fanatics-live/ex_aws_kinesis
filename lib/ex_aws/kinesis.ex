defmodule ExAws.Kinesis do
  @moduledoc """
  Operations on AWS Kinesis

  http://docs.aws.amazon.com/kinesis/latest/APIReference/API_Operations.html
  """

  import ExAws.Utils, only: [camelize_keys: 1, upcase: 1]
  require Logger

  @namespace "Kinesis_20131202"

  ## Streams
  ######################

  @type name_or_arn :: [arn: String.t()] | [name: String.t()]

  @type stream_name :: binary

  @doc "Lists streams"
  @spec list_streams() :: ExAws.Operation.JSON.t()
  def list_streams() do
    request(:list_streams, %{})
  end

  @doc "Describe Stream"
  @type describe_stream_opts :: [
          {:limit, pos_integer}
          | {:exclusive_start_shard_id, binary}
        ]
  @spec describe_stream(stream_name :: stream_name | name_or_arn(), opts :: describe_stream_opts) ::
          ExAws.Operation.JSON.t()
  def describe_stream(stream_name, opts \\ []) do
    data =
      opts
      |> camelize_keys
      |> Map.merge(name_args(stream_name))

    request(:describe_stream, data)
  end

  @doc "Describe stream summary"
  @spec describe_stream_summary(stream_name :: stream_name | name_or_arn) ::
          ExAws.Operation.JSON.t()
  def describe_stream_summary(stream_name) do
    request(:describe_stream_summary, name_args(stream_name))
  end

  @doc "Creates stream"
  @spec create_stream(stream_name :: stream_name | name_or_arn, shard_count :: pos_integer) ::
          ExAws.Operation.JSON.t()
  def create_stream(stream_name, shard_count \\ 1) do
    data =
      stream_name
      |> name_args()
      |> Map.put("ShardCount", shard_count)

    request(:create_stream, data)
  end

  @doc "Deletes stream"
  @spec delete_stream(stream_name :: stream_name | name_or_arn) :: ExAws.Operation.JSON.t()
  def delete_stream(stream_name) do
    request(:delete_stream, name_args(stream_name))
  end

  ## Records
  ######################

  @doc "Get stream records"
  @type get_records_opts :: [
          {:limit, pos_integer},
          {:arn, String.t()}
        ]
  @spec get_records(shard_iterator :: binary, opts :: get_records_opts) ::
          ExAws.Operation.JSON.t()
  def get_records(shard_iterator, opts \\ []) do
    data =
      opts
      |> camelize_keys()
      |> Map.merge(%{"ShardIterator" => shard_iterator})

    data =
      if arn = opts[:arn] do
        Map.put(data, "StreamARN", arn)
      else
        data
      end

    request(:get_records, data, %{parser: &decode_records/1})
  end

  def decode_records({:ok, %{"Records" => records} = results}) do
    {:ok, %{results | "Records" => do_decode_records(records)}}
  end

  def decode_records(result), do: result

  defp do_decode_records(records) do
    Enum.map(records, fn %{"Data" => data} = record ->
      %{record | "Data" => Base.decode64!(data)}
    end)
  end

  @doc "Puts a record on a stream"
  @type put_record_opts :: [
          {:explicit_hash_key, binary}
          | {:sequence_number_for_ordering, binary}
        ]
  @spec put_record(
          stream_name :: stream_name | name_or_arn,
          partition_key :: binary,
          data :: binary,
          opts :: put_record_opts
        ) :: ExAws.Operation.JSON.t()
  def put_record(stream_name, partition_key, data, opts \\ []) do
    data =
      opts
      |> camelize_keys()
      |> Map.merge(name_args(stream_name))
      |> Map.merge(%{
        "Data" => Base.encode64(data),
        "PartitionKey" => partition_key
      })

    request(:put_record, data)
  end

  @doc "Put multiple records on a stream"
  @type put_records_record :: [
          {:data, binary}
          | {:explicit_hash_key, binary}
        ]
  @spec put_records(stream_name :: stream_name, records :: [put_records_record]) ::
          ExAws.Operation.JSON.t()
  def put_records(stream_name, records) when is_list(records) do
    data =
      stream_name
      |> name_args()
      |> Map.put("Records", Enum.map(records, &format_record/1))

    request(:put_records, data)
  end

  defp format_record(%{data: data, partition_key: partition_key} = record) do
    formatted = %{"Data" => Base.encode64(data), "PartitionKey" => partition_key}

    case record do
      %{explicit_hash_key: hash_key} ->
        Map.put(formatted, "ExplicitHashKey", hash_key)

      _ ->
        formatted
    end
  end

  ## Shards
  ######################

  @doc """
  Get a shard iterator
  """
  @type shard_iterator_types ::
          :at_sequence_number
          | :after_sequence_number
          | :trim_horizon
          | :latest
  @type get_shard_iterator_opts :: [
          {:starting_sequence_number, binary}
        ]
  @spec get_shard_iterator(
          stream_name :: stream_name | name_or_arn,
          shard_id :: binary,
          shard_iterator_type :: shard_iterator_types,
          opts :: get_shard_iterator_opts
        ) :: ExAws.Operation.JSON.t()
  def get_shard_iterator(stream_name, shard_id, shard_iterator_type, opts \\ []) do
    data =
      opts
      |> Map.new()
      |> camelize_keys
      |> Map.merge(name_args(stream_name))
      |> Map.merge(%{
        "ShardId" => shard_id,
        "ShardIteratorType" => upcase(shard_iterator_type)
      })

    request(:get_shard_iterator, data)
  end

  @doc "Merge adjacent shards"
  @spec merge_shards(
          stream_name :: stream_name | name_or_arn,
          adjacent_shard_id :: binary,
          shard_id :: binary
        ) ::
          ExAws.Operation.JSON.t()
  def merge_shards(stream_name, adjacent_shard, shard) do
    data =
      stream_name
      |> name_args()
      |> Map.merge(%{
        "AdjacentShardToMerge" => adjacent_shard,
        "ShardToMerge" => shard
      })

    request(:merge_shards, data)
  end

  @doc "Split a shard"
  @spec split_shard(
          stream_name :: stream_name | name_or_arn,
          shard :: binary,
          new_starting_hash_key :: binary
        ) ::
          ExAws.Operation.JSON.t()
  def split_shard(stream_name, shard, new_starting_hash_key) do
    data =
      stream_name
      |> name_args()
      |> Map.merge(%{
        "ShardToSplit" => shard,
        "NewStartingHashKey" => new_starting_hash_key
      })

    request(:split_shard, data)
  end

  ## Tags
  ######################

  @type stream_tags :: [{atom, binary} | {binary, binary}] | %{(atom | binary) => binary}

  @doc "Add tags to stream"
  @spec add_tags_to_stream(stream_name :: stream_name | name_or_arn, tags :: stream_tags) ::
          ExAws.Operation.JSON.t()
  def add_tags_to_stream(stream_name, tags) do
    data =
      stream_name
      |> name_args()
      |> Map.put("Tags", Map.new(tags))

    request(:add_tags_to_stream, data)
  end

  @type list_tags_for_stream_opts :: [
          {:limit, pos_integer}
          | {:exclusive_start_tag_key, binary}
        ]

  @doc "List tags for a stream"
  @spec list_tags_for_stream(
          stream_name :: stream_name | name_or_arn,
          opts :: list_tags_for_stream_opts
        ) :: ExAws.Operation.JSON.t()
  def list_tags_for_stream(stream_name, opts \\ []) do
    data =
      opts
      |> Map.new()
      |> camelize_keys()
      |> Map.merge(name_args(stream_name))

    request(:list_tags_for_stream, data)
  end

  @doc "Remove tags from stream"
  @spec remove_tags_from_stream(stream_name :: stream_name | name_or_arn, tag_keys :: [binary]) ::
          ExAws.Operation.JSON.t()
  def remove_tags_from_stream(stream_name, tag_keys) do
    data =
      stream_name
      |> name_args()
      |> Map.put("TagKeys", tag_keys)

    request(:remove_tags_from_stream, data)
  end

  defp request(action, data, opts \\ %{}) do
    operation =
      action
      |> Atom.to_string()
      |> Macro.camelize()

    ExAws.Operation.JSON.new(
      :kinesis,
      %{
        data: data,
        headers: [
          {"x-amz-target", "#{@namespace}.#{operation}"},
          {"content-type", "application/x-amz-json-1.1"}
        ]
      }
      |> Map.merge(opts)
    )
  end

  defp name_args(arn: arn), do: %{"StreamARN" => arn}
  defp name_args(name: name), do: %{"StreamName" => name}
  defp name_args(name) when is_binary(name), do: %{"StreamName" => name}
end
