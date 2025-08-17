defmodule NGram do
  @moduledoc """
  # NGram – Three-Word Sequence Counter

  Counts the **100 most common three-word sequences** from files or stdin.

  ## Highlights
  - Files or stdin (`-`)
  - Case-insensitive
  - Keeps contractions & singular possessives; normalizes plural possessives (`dogs'` → `dogs`)
  - Preserves hyphenated words (incl. en/em dashes normalized to `-`)
  - Parallel per-file processing with boundary stitching
  - Mnesia-backed aggregation with low-memory ranking via external sort

  ## Examples
      elixir solution.exs texts/moby_dick.txt
      elixir solution.exs texts/moby_dick.txt texts/brothers-karamazov.txt
      cat texts/*.txt | elixir solution.exs -
  """

  @n 3
  @top_k 100
  @default_lines_per_chunk 10_000
  @max_concurrent_workers 8

  # ===========================
  # Public: simple (non-parallel) per-paths
  # ===========================
  @spec run([String.t()]) :: :ok | :noop
  def run(paths) when is_list(paths) and paths != [] do
    Enum.each(paths, fn path ->
      IO.puts("Processing: #{path}\n")

      path
      |> token_stream()
      |> count_ngrams(@n)
      |> top_k(@top_k)
      |> Enum.each(fn {phrase, count} ->
        IO.puts("#{phrase} - #{count}")
      end)

      IO.puts("")
    end)
  end

  # ===========================
  # Tokenization
  # ===========================
  @spec token_stream(String.t() | [String.t()]) :: Enumerable.t()
  def token_stream(path) when is_binary(path) do
    path
    |> File.stream!()
    |> Stream.flat_map(&line_to_tokens/1)
  end

  def token_stream(paths) do
    paths
    |> Stream.flat_map(&File.stream!/1)
    |> Stream.flat_map(&line_to_tokens/1)
  end

  @spec token_stream_from_string(String.t()) :: Enumerable.t()
  def token_stream_from_string(content) when is_binary(content) do
    content
    |> String.split(~r/\R/u, trim: false)
    |> Stream.flat_map(&line_to_tokens/1)
  end

  def line_to_tokens(line) do
    line
    |> String.downcase()
    # curly → '
    |> String.replace(~r/[’‘‛′`]/u, "'")
    # dash runs outside words → space
    |> String.replace(~r/(?<![\p{L}\p{N}])[-–—]+(?![\p{L}\p{N}])/u, " ")
    # en/em dash inside words → '-'
    |> String.replace(~r/[–—]/u, "-")
    # Remove control characters
    |> String.replace(~r/[\x00-\x1F\x7F]+/u, " ")
    # drop other punct
    |> String.replace(~r/[^\p{L}\p{N}'\- ]+/u, " ")
    |> String.split()
    |> Enum.map(&normalize_token/1)
    |> Enum.filter(&(&1 != ""))
  end

  defp normalize_token(word) do
    word
    # 1: Trim leading/trailing single quotes
    |> String.trim("'")
    # 2: Trim leading/trailing hyphens
    |> String.trim("-")
    # 3: Replace multiple hyphens with a single hyphen
    |> then(&Regex.replace(~r/-{2,}/, &1, "-"))
    # 4: Remove single quotes not followed by 's' or 't'
    |> then(&Regex.replace(~r/'(?![st])/u, &1, ""))
  end

  # ===========================
  # Counting
  # ===========================
  @doc """
  Counts n-grams from a token stream (single pass). Maintains carry of last (n-1) tokens.
  """
  @spec count_ngrams(Enumerable.t(), pos_integer()) :: map()
  def count_ngrams(token_stream, n) when n >= 1 do
    {counts, _carry} =
      Enum.reduce(token_stream, {%{}, []}, fn token, {counts, carry} ->
        window = (carry ++ [token]) |> take_last(n)

        counts =
          if length(window) == n do
            key = Enum.join(window, " ")
            Map.update(counts, key, 1, fn v -> v + 1 end)
          else
            counts
          end

        {counts, take_last(carry ++ [token], n - 1)}
      end)

    counts
  end

  def take_last(_list, k) when k <= 0, do: []

  def take_last(list, k) do
    drop = max(length(list) - k, 0)
    Enum.drop(list, drop)
  end

  # ===========================
  # Ranking
  # ===========================
  @doc """
  Returns top k as {phrase, count} sorted by count desc, then phrase asc.
  """
  @spec top_k(map(), non_neg_integer()) :: [{String.t(), non_neg_integer()}]
  def top_k(counts_map, k) do
    counts_map
    |> Enum.sort_by(fn {phrase, count} -> {-count, phrase} end)
    |> Enum.take(k)
  end

  # ===========================
  # Parallel per-file with Mnesia aggregation
  # ===========================
  def run_parallel_file(path, opts \\ []) do
    IO.inspect(opts)
    n = @n
    lines_per_chunk = Keyword.get(opts, :lines_per_chunk, @default_lines_per_chunk)
    workers = Keyword.get(opts, :workers, @max_concurrent_workers)
    debug = Keyword.get(opts, :debug, false)

    IO.puts(
      "Processing (parallel: workers=#{workers}, chunk_lines=#{lines_per_chunk}): #{path}\n"
    )

    # Capture start time
    start_time = DateTime.utc_now()
    IO.puts("Start time: #{DateTime.to_string(start_time)}")

    ensure_mnesia!()
    create_or_clear_table!()

    chunks =
      File.stream!(path)
      |> Stream.chunk_every(lines_per_chunk)
      |> Stream.with_index()
      |> Task.async_stream(
        fn {lines, chunk_index} ->
          result = count_chunk_no_overlap(lines, n, debug: debug)

          Enum.each(result.counts, fn {phrase, count} ->
            if is_binary(phrase) and phrase != "" do
              try do
                :mnesia.dirty_update_counter(:ngrams, phrase, count)
              catch
                error -> IO.puts(:stderr, "Mnesia write error for #{phrase}: #{inspect(error)}")
              end
            end
          end)

          %{head: result.head, tail: result.tail, idx: chunk_index}
        end,
        max_concurrency: workers,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, r} -> r end)
      |> Enum.sort_by(& &1.idx)

    IO.inspect(chunks)

    stitch_adjacent_chunks(chunks, n)
    export_and_rank!(@top_k)
    IO.puts("")

    # Capture end time and calculate duration
    end_time = DateTime.utc_now()
    duration = DateTime.diff(end_time, start_time, :second)

    duration_str =
      if duration >= 60, do: "#{div(duration, 60)}m #{rem(duration, 60)}s", else: "#{duration}s"

    IO.puts("End time: #{DateTime.to_string(end_time)}")
    IO.puts("Duration: #{duration_str}")

    # Ensure clean shutdown
    :mnesia.stop()
  end

  # Count only windows fully inside the chunk; return head/tail for stitching
  defp count_chunk_no_overlap(lines, n, opts) do
    # IO.inspect(lines)
    debug = Keyword.get(opts, :debug, false)
    pid = inspect(self())

    {counts, head_tokens, carry} =
      Enum.reduce(lines, {%{}, [], []}, fn line, {counts, head, carry} ->
        tokens = line_to_tokens(line)
        # IO.inspect(line, label: "#{pid} line")
        # IO.inspect(tokens, label: "#{pid} tokens")

        if tokens == [] do
          # Skip empty lines
          {counts, head, carry}
        else
          head = if head == [], do: Enum.take(tokens, n - 1), else: head
          # IO.inspect(head, label: "#{pid} head")
          # IO.inspect(counts, label: "#{pid} counts")
          # IO.inspect(carry, label: "#{pid} carry")

          {counts, carry} =
            Enum.reduce(tokens, {counts, carry}, fn tok, {m, c} ->
              # IO.inspect({tok, m, c})
              win = take_last(c ++ [tok], n)
              # IO.inspect(win, label: "#{pid} win")

              m =
                if length(win) == n do
                  key = Enum.join(win, " ")
                  Map.update(m, key, 1, fn v -> v + 1 end)
                else
                  m
                end

              # IO.inspect(take_last(c ++ [tok], n - 1), label: "take last 2 of carry ready for new token")

              {m, take_last(c ++ [tok], n - 1)}
            end)

          {counts, head, carry}
        end
      end)

    if debug do
      IO.inspect(head_tokens, label: "DEBUG #{pid} head")
      IO.inspect(carry, label: "DEBUG #{pid} tail")
      IO.inspect(counts, label: "DEBUG #{pid} counts")
    end

    %{counts: counts, head: head_tokens, tail: carry}
  end

  # For k in 1..n-1 stitch tail_k(left) + head_(n-k)(right)
  defp stitch_adjacent_chunks(chunks, n) do
    chunks
    # Chunk every 2 elements and step 1 at a time to form pairs
    # discard any element that doesn't fit in a pair
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.with_index()
    |> Enum.each(fn {[left, right], _i} ->
      # IO.inspect(chunked, label: "chunked")
      Enum.each(1..(n - 1), fn k ->
        left_needed = k
        right_needed = n - k

        # IO.inspect(k, label: "left_needed k")
        # IO.inspect(n - k, label: "right_needed n - k")

        if length(left.tail) >= left_needed and length(right.head) >= right_needed do
          left_part = take_last(left.tail, left_needed)
          right_part = Enum.take(right.head, right_needed)
          key = Enum.join(left_part ++ right_part, " ")

          # IO.inspect(key, label: "stiched trigram")

          if is_binary(key) and key != "" do
            :mnesia.dirty_update_counter(:ngrams, key, 1)
          end
        end
      end)
    end)
  end

  # ---------------------------
  # Mnesia helpers
  # ---------------------------
  defp ensure_mnesia! do
    case :mnesia.create_schema([node()]) do
      :ok -> :ok
      {:error, {_, {:already_exists, _}}} -> :ok
      {:error, _} -> :ok
    end

    :mnesia.start()
  end

  defp create_or_clear_table!() do
    :mnesia.create_schema([node()])
    :mnesia.start()

    case :mnesia.create_table(:ngrams,
           attributes: [:phrase, :count],
           disc_copies: [node()],
           type: :set
         ) do
      {:atomic, :ok} ->
        :ok

      {:aborted, {:already_exists, :ngrams}} ->
        :ok

      other ->
        raise "mnesia create_table failed: #{inspect(other)}"
    end

    :mnesia.clear_table(:ngrams)
  end

  defp export_and_rank!(k) do
    tmp = "ngram_counts.txt"
    {:ok, io} = File.open(tmp, [:write, :utf8])

    :mnesia.activity(:async_dirty, fn ->
      walk = fn walk_fun, key ->
        case key do
          :"$end_of_table" ->
            :ok

          _ ->
            for {:ngrams, phrase, count} <- :mnesia.dirty_read(:ngrams, key) do
              IO.write(io, "#{phrase}\t#{count}\n")
            end

            walk_fun.(walk_fun, :mnesia.dirty_next(:ngrams, key))
        end
      end

      first = :mnesia.dirty_first(:ngrams)
      walk.(walk, first)
    end)

    :file.sync(io)
    File.close(io)

    # phrase-sort -> aggregate -> count-sort -> top K -> print
    script = """
    sort -k1,1 #{tmp} 2> /dev/null \
    | awk -F'\\t' '
      BEGIN { prev=""; sum=0 }
      NF != 2 || $2 !~ /^[0-9]+$/ { next }
      prev==$1 { sum+=$2 }
      prev!=$1 { if (NR>1 && prev != "") printf "%d\\t%s\\n", sum, prev; prev=$1; sum=$2 }
      END { if (prev != "") printf "%d\\t%s\\n", sum, prev }
    ' 2> /dev/null \
    | sort -k1,1nr -k2,2 2> /dev/null \
    | head -#{k} \
    | awk -F'\\t' '{print $2 " - " $1}' 2> /dev/null
    """

    case System.cmd("bash", ["-c", script], stderr_to_stdout: true) do
      {output, 0} ->
        IO.write(output)

      {err, code} ->
        IO.warn("ranking failed (exit #{code}): #{err}")
    end

    File.rm(tmp)
  end
end

defmodule NGram.CLI do
  def main(args) do
    cond do
      Enum.member?(args, "--test") ->
        ExUnit.start()

        defmodule NGramTests do
          use ExUnit.Case, async: true

          defp write_tmp!(name, contents) do
            path = Path.join(System.tmp_dir!(), name)
            File.write!(path, contents)
            path
          end

          test "take_last returns last k elements" do
            assert NGram.take_last([1, 2, 3, 4], 2) == [3, 4]
            assert NGram.take_last([1, 2, 3], 10) == [1, 2, 3]
            assert NGram.take_last([1, 2, 3], 0) == []
            assert NGram.take_last([], 3) == []
          end

          test "simple repeated letters example" do
            text = "a a a a a a a b b b b b c c c c"
            path = write_tmp!("simple.txt", text)

            counts =
              [path]
              |> NGram.token_stream()
              |> NGram.count_ngrams(3)

            assert Map.get(counts, "a a a") == 5
            assert Map.get(counts, "a a b") == 1
            assert Map.get(counts, "b b b") == 3
            assert Map.get(counts, "c c c") == 2

            [{top_phrase, top_count} | _] = NGram.top_k(counts, 1)
            assert top_phrase == "a a a"
            assert top_count == 5
          end

          test "predictable 10..1 ranking" do
            tokens =
              Enum.flat_map(10..1, fn count ->
                letter = <<?z - (10 - count)>>
                triple = [letter, letter, letter]
                Enum.flat_map(1..count, fn idx -> triple ++ ["x#{letter}#{idx}"] end)
              end)

            text = Enum.join(tokens, " ")
            path = write_tmp!("predictable_ranking.txt", text)

            counts =
              [path]
              |> NGram.token_stream()
              |> NGram.count_ngrams(3)

            ranked = NGram.top_k(counts, 10)

            expected =
              Enum.map(10..1, fn count ->
                letter = <<?z - (10 - count)>>
                {Enum.join([letter, letter, letter], " "), count}
              end)

            assert ranked == expected
          end

          test "line_to_tokens handles Moby-Dick-style text" do
            # Realistic input with contractions, hyphens, punctuation
            input =
              "Ishmael’s voyage, isn’t it? Rock-n--roll! --- Call me Ishmael. This is a -test- 'and' it isn't real."

            expected = [
              "ishmael's",
              "voyage",
              "isn't",
              "it",
              "rock-n-roll",
              "call",
              "me",
              "ishmael",
              "this",
              "is",
              "a",
              "test",
              "and",
              "it",
              "isn't",
              "real"
            ]

            # Write to temp file and process
            path = write_tmp!("moby_dick_sample.txt", input)
            result = File.stream!(path) |> Enum.flat_map(&NGram.line_to_tokens/1)
            assert result == expected, "Expected #{inspect(expected)}, got #{inspect(result)}"
          end
        end

      args == ["-"] ->
        input = IO.read(:stdio, :all)

        input
        |> NGram.token_stream_from_string()
        |> NGram.count_ngrams(3)
        |> NGram.top_k(100)
        |> Enum.each(fn {p, c} -> IO.puts("#{p} - #{c}") end)

      args == [] ->
        defaults = [
          "/home/coderpad/data/brothers-karamazov.txt",
          "/home/coderpad/data/moby-dick.txt"
        ]

        paths = Enum.filter(defaults, &File.exists?/1)

        if paths == [] do
          IO.puts(:stderr, "No default .txt files found in /home/coderpad/data")
          System.halt(1)
        end

        Enum.each(paths, fn p ->
          NGram.run_parallel_file(p, workers: 10, lines_per_chunk: 1000, debug: false)
        end)

      true ->
        paths =
          args
          |> Enum.reject(&String.starts_with?(&1, "--"))
          |> Enum.filter(&File.exists?/1)

        if paths == [] do
          IO.puts(
            :stderr,
            "Usage: elixir solution.exs <file> [<file> ...] | cat <file> | elixir solution.exs - | elixir solution.exs --test"
          )

          System.halt(1)
        end

        Enum.each(paths, fn p ->
          NGram.run_parallel_file(p, workers: 10, lines_per_chunk: 1000, debug: false)
        end)
    end
  end
end

NGram.CLI.main(System.argv())
