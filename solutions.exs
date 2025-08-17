defmodule NGram do
  # Code.put_compiler_option(:docs, true)

  @moduledoc """
  # NGram – Three-Word Sequence Counter

  This program counts the **100 most common three-word sequences** in one or more text files, or from stdin.

  ## Features

  - Accepts **one or more file paths** as arguments:
    ```bash
    elixir solution.exs texts/moby_dick.txt texts/brothers-karamazov.txt
    ```
  - Accepts **stdin input**:
    ```bash
    cat texts/*.txt | elixir solution.exs
    ```
    Or explicitly:
    ```bash
    cat texts/*.txt | elixir solution.exs -
    ```
  - Outputs the **first 100 most common three-word sequences**.
  - Ignores punctuation and treats line endings as whitespace.
  - Is **case-insensitive**: `"LOVE"` == `"love"`.
  - Preserves **contractions**, **singular possessives**, and **hyphenated words**.
    - Example: `"Fido's bark isn't well-liked"` → `[ "fido's", "bark", "isn't", "well-liked" ]`
  - Strips **plural possessive apostrophes**:
    - `"dogs' tails wag"` → `[ "dogs", "tails", "wag" ]`
  - Handles Unicode gracefully by replacing with spaces (customizable).
  - Efficient and capable of handling large files quickly.

  ## Examples

  **From a file:**
  ```bash
  elixir solution.exs texts/moby_dick.txt
  ```

  **Multiple files:**
  ```bash
  elixir solution.exs texts/moby_dick.txt texts/brothers-karamazov.txt
  ```

  **From stdin:**
  ```bash
  cat texts/moby_dick.txt | elixir solution.exs
  ```
  Or:
  ```bash
  cat texts/moby_dick.txt | elixir solution.exs -
  ```

  ## Running Tests in CoderPad

  Tests are built-in.
  To run them in CoderPad:
  ```bash
  elixir solution.exs --test
  ```
  This runs `ExUnit` inline, without needing separate files.

  ### Included Tests
  1. **Simple repeated letters** – verifies correct n-gram counting.
  2. **Predictable ranking** – verifies correct sorting and limiting to top 10.
  3. **Hyphen and apostrophe handling** – validates tokenizer rules.

  ## Output Format

  Output is printed as:
  ```
  word1 word2 word3 - <count>
  ```
  Example:
  ```
  the whale and - 57
  in the boat - 43
  ...
  ```

  ## Notes
  - Hyphens at line endings are treated as punctuation (permitted by requirements).
  - Can be modified to stream from files instead of loading fully if extreme scalability is required.
  """

  @n 3
  @top_k 100

  @spec run_parallel([String.t()]) :: :ok | :noop
  def run_parallel(paths) when is_list(paths) and paths != [] do
    n = @n
    workers = System.schedulers_online()
    lines_per_chunk = 1000
    debug = false

    # Capture start time
    start_time = DateTime.utc_now()
    IO.puts("Start time: #{DateTime.to_string(start_time)}")
    IO.puts("Workers: #{workers}")
    IO.puts("Lines per chunk: #{lines_per_chunk}")

    Enum.each(paths, fn path ->
      IO.puts("Processing: #{path}\n")

      chunks =
        path
        |> File.stream!()
        |> Stream.chunk_every(lines_per_chunk)
        |> Stream.with_index()
        |> Task.async_stream(
          fn {lines, index} ->
            count_chunk_no_overlap(lines, n, index, debug: debug)
          end,
          max_concurrency: workers,
          timeout: :infinity
        )
        |> Enum.map(fn {:ok, r} -> r end)
        |> Enum.sort_by(& &1.idx)

      stitched_counts = stitch_adjacent_chunks(chunks, n)

      chunks
      |> Enum.reduce(%{}, fn %{counts: c}, acc ->
        merge_counts(c, acc)
      end)
      |> merge_counts(stitched_counts)
      |> top_k(@top_k)
      |> Enum.each(fn {trigram, count} -> IO.puts("#{trigram} - #{count}") end)

      # spacer between files
      IO.puts("")
    end)

    end_time = DateTime.utc_now()
    duration = DateTime.diff(end_time, start_time, :millisecond)

    IO.puts("End time: #{DateTime.to_string(end_time)}")
    IO.puts("Duration: #{duration} milliseconds")
  end

  defp merge_counts(c1, c2) when is_map(c1) and is_map(c2) do
    if map_size(c1) <= map_size(c2) do
      Enum.reduce(c1, c2, fn {k, v}, acc -> Map.update(acc, k, v, &(&1 + v)) end)
    else
      Enum.reduce(c2, c1, fn {k, v}, acc -> Map.update(acc, k, v, &(&1 + v)) end)
    end
  end

  # Count only windows fully inside the chunk; return head/tail for stitching
  defp count_chunk_no_overlap(lines, n, chunk_index, opts) do
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
              # current window candidate
              base = c ++ [tok]

              # only when we actually have n tokens
              m =
                if length(base) == n do
                  key = Enum.join(base, " ")
                  Map.update(m, key, 1, &(&1 + 1))
                else
                  m
                end

              # update carry for next token
              {m, take_last(base, n - 1)}
            end)

          {counts, head, carry}
        end
      end)

    if debug do
      IO.inspect(head_tokens, label: "DEBUG #{pid} head")
      IO.inspect(carry, label: "DEBUG #{pid} tail")
      IO.inspect(counts, label: "DEBUG #{pid} counts")
    end

    %{counts: counts, head: head_tokens, tail: carry, idx: chunk_index}
  end

  # For k in 1..n-1 stitch tail_k(left) + head_(n-k)(right)
  defp stitch_adjacent_chunks(chunks, n) do
    chunks
    # Chunk every 2 elements and step 1 at a time to form pairs
    # discard any element that doesn't fit in a pair
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.reduce(%{}, fn [left, right], acc ->
      # IO.inspect(acc, label: "chunked")

      Enum.reduce(1..(n - 1), acc, fn k, acc1 ->
        left_needed = k
        right_needed = n - k

        # IO.inspect(k, label: "left_needed k")
        # IO.inspect(n - k, label: "right_needed n - k")

        if length(left.tail) >= left_needed and length(right.head) >= right_needed do
          left_part = take_last(left.tail, left_needed)
          right_part = Enum.take(right.head, right_needed)
          key = Enum.join(left_part ++ right_part, " ")

          # IO.inspect(key, label: "stiched trigram")

          Map.update(acc1, key, 1, &(&1 + 1))
        else
          acc1
        end
      end)
    end)
  end

  # --- Tokenization ---------------------------------------------------------

  @doc """
  Returns a *stream* of lowercase tokens from all files, in order.
  Rules:
    * Case-insensitive (downcase).
    * Hyphens => treated as spaces (split words).
    * Punctuation removed, except apostrophes *inside* a word are kept (don't, fido's).
    * A trailing possessive apostrophe in plural (e.g., dogs') normalizes to 'dogs'.
  """
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

  defp line_to_tokens(line) do
    line
    |> String.downcase()
    # normalize curly quotes to '
    |> String.replace(~r/[’‘‛′`]/u, "'")
    # Replace dash runs that are NOT between letters/digits -> space
    |> String.replace(~r/(?<![\p{L}\p{N}])[-–—]+(?![\p{L}\p{N}])/u, " ")
    # Normalize en/em dashes that ARE between letters to simple hyphen (so we keep them)
    |> String.replace(~r/[–—]/u, "-")
    # Drop other punctuation; allow letters/digits/apostrophes/hyphen/space
    |> String.replace(~r/[^\p{L}\p{N}'\- ]+/u, " ")
    |> String.split()
    |> Enum.map(&normalize_token/1)
    |> Enum.filter(&(&1 != ""))
  end

  defp normalize_token(word) do
    word
    # Trim stray apostrophes or hyphens at the ends (e.g., "dogs'" -> "dogs", "well-" -> "well")
    |> String.trim("'")
    |> String.trim("-")
    # Collapse doubled apostrophes
    |> then(&Regex.replace(~r/'{2,}/, &1, "'"))
    # Plural possessive: dogs' -> dogs (also works for unicode letters/digits)
    |> then(fn w -> Regex.replace(~r/([\p{L}\p{N}])'$/u, w, "\\1") end)
  end

  # --- N-gram counting ------------------------------------------------------

  @doc """
  Counts 3-grams from a short string.

  ## Examples

      iex> "a a a a"
      ...> |> NGram.token_stream_from_string()
      ...> |> NGram.count_ngrams(3)
      ...> |> Map.get("a a a")
      2
  """
  @spec count_ngrams(Enumerable.t(), pos_integer()) :: map()
  def count_ngrams(token_stream, n) when n >= 1 do
    {counts, _carry} =
      Enum.reduce(token_stream, {%{}, []}, fn token, {counts, carry} ->
        base = carry ++ [token]

        counts =
          if length(base) == n do
            key = Enum.join(base, " ")
            Map.update(counts, key, 1, &(&1 + 1))
          else
            counts
          end

        {counts, take_last(base, n - 1)}
      end)

    counts
  end

  def take_last(_list, k) when k <= 0, do: []

  def take_last(list, k) do
    drop = max(length(list) - k, 0)
    Enum.drop(list, drop)
  end

  # --- Ranking --------------------------------------------------------------

  @doc """
  Return the top k n-grams as {phrase, count} sorted by:
    1) descending count, then
    2) lexicographic phrase (stable, deterministic).
  """
  @spec top_k(map(), non_neg_integer()) :: [{String.t(), non_neg_integer()}]
  def top_k(counts_map, k) do
    counts_map
    |> Enum.sort_by(fn {phrase, count} -> {-count, phrase} end)
    |> Enum.take(k)
  end
end

defmodule RunnerConfig do
  @moduledoc false
  # set to false if you want Run to process a file instead
  @run_tests false
  def run_tests?, do: @run_tests
end

# ---- decide mode once ----
args = System.argv()
run_tests = RunnerConfig.run_tests?() or "--test" in args

# ---- start ExUnit & define tests only if testing ----
if run_tests do
  ExUnit.start(trace: true)

  defmodule NGramInlineTest do
    use ExUnit.Case, async: true
    # doctest NGram

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

      # 7-3+1
      assert Map.get(counts, "a a a") == 5
      # crosses a→b once
      assert Map.get(counts, "a a b") == 1
      # 5-3+1
      assert Map.get(counts, "b b b") == 3
      # 4-3+1
      assert Map.get(counts, "c c c") == 2

      [{top_phrase, top_count} | _] = NGram.top_k(counts, 1)
      assert top_phrase == "a a a"
      assert top_count == 5
    end

    test "simple repeated letters ranking" do
      # 7× a, 5× b, 4× c
      text = "a a a a a a a b b b b b c c c c"
      path = write_tmp!("simple_ranking.txt", text)

      counts =
        [path]
        |> NGram.token_stream()
        |> NGram.count_ngrams(3)

      ranked = NGram.top_k(counts, 4)

      # Expected:
      #   1. "a a a" = 5 times
      #   2. "b b b" = 3 times
      #   3. "c c c" = 2 times
      #   4. The remaining 3-grams (like "a a b") sorted lexicographically for ties
      assert Enum.at(ranked, 0) == {"a a a", 5}
      assert Enum.at(ranked, 1) == {"b b b", 3}
      assert Enum.at(ranked, 2) == {"c c c", 2}

      # Ensure the list is sorted by count desc, then phrase asc
      sorted_again =
        counts
        |> Enum.sort_by(fn {phrase, count} -> {-count, phrase} end)
        |> Enum.take(4)

      assert ranked == sorted_again
    end

    test "predictable 10..1 ranking with lex-safe letters" do
      # Letters z..q (10 letters): z=10, y=9, ..., q=1
      # Insert one unique token after each triple to prevent window-bridging counts.
      tokens =
        Enum.flat_map(10..1, fn count ->
          # z, y, x, w, v, u, t, s, r, q
          letter = <<?z - (10 - count)>>
          triple = [letter, letter, letter]

          Enum.flat_map(1..count, fn idx ->
            # unique per occurrence
            triple ++ ["x#{letter}#{idx}"]
          end)
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

    test "keeps contractions with curly quotes and normal apostrophes" do
      path =
        write_tmp!("curly.txt", """
        Don’t stop believing. Don't stop.
        Fido’s bowl isn’t empty.
        """)

      tokens = NGram.token_stream([path]) |> Enum.to_list()
      assert "don't" in tokens
      assert "isn't" in tokens
      assert "fido's" in tokens
    end

    test "hyphenated words are preserved (normalize en/em dashes)" do
      path = write_tmp!("hyphens.txt", "rock-n-roll rock—n—roll rock–n–roll well-liked")
      tokens = NGram.token_stream([path]) |> Enum.to_list()

      # All variants normalize to a simple hyphen and remain single tokens
      assert Enum.frequencies(tokens) == %{"rock-n-roll" => 3, "well-liked" => 1}

      # Sanity: none of these should appear as split pieces
      refute Enum.any?(tokens, &(&1 in ["rock", "n", "roll", "well", "liked"]))
    end

    test "plural possessive dogs' -> dogs" do
      path = write_tmp!("possessive.txt", "Dogs' collars shine. dogs' collars shine!")
      tokens = NGram.token_stream([path]) |> Enum.to_list()
      refute Enum.any?(tokens, &(&1 == "dogs'"))
      assert Enum.count(tokens, &(&1 == "dogs")) >= 2

      counts =
        [path]
        |> NGram.token_stream()
        |> NGram.count_ngrams(3)

      assert Map.get(counts, "dogs collars shine") == 2
    end

    test "keeps Unicode letters (Païssy)" do
      path = write_tmp!("unicode.txt", "Father Païssy spoke softly.")
      tokens = NGram.token_stream([path]) |> Enum.to_list()
      assert "païssy" in tokens
    end

    test "3-gram counting spans files" do
      a = write_tmp!("a.txt", "the sperm\n")
      b = write_tmp!("b.txt", "whale the sperm whale\n")

      counts =
        [a, b]
        |> NGram.token_stream()
        |> NGram.count_ngrams(3)

      assert Map.get(counts, "the sperm whale") == 2
    end

    test "top_k ordering on ties" do
      counts = %{
        "alpha beta gamma" => 3,
        "alpha beta delta" => 3,
        "zeta eta theta" => 2
      }

      ranked = NGram.top_k(counts, 10)

      assert ranked == [
               {"alpha beta delta", 3},
               {"alpha beta gamma", 3},
               {"zeta eta theta", 2}
             ]
    end
  end
end

# ---- CLI behavior ----
cond do
  run_tests ->
    # tests already started above
    :ok

  # optional: explicit stdin mode if you want it
  args == ["-"] ->
    input = IO.read(:stdio, :all)

    input
    # add this helper (below)
    |> NGram.token_stream_from_string()
    |> NGram.count_ngrams(3)
    |> NGram.top_k(100)
    |> Enum.each(fn {p, c} -> IO.puts("#{p} - #{c}") end)

  args == [] ->
    defaults = [
      "/home/coderpad/data/brothers-karamazov.txt",
      "/home/coderpad/data/moby-dick.txt"
    ]

    paths =
      defaults
      |> Enum.filter(&File.exists?/1)
      |> Enum.uniq()

    case paths do
      [] ->
        IO.puts(:stderr, "No default .txt files found in /home/coderpad/data")
        System.halt(1)

      paths ->
        IO.puts("Run at: #{NaiveDateTime.utc_now()}")
        NGram.run_parallel(paths)
    end

  true ->
    NGram.run_parallel(Enum.reject(args, &(&1 == "--test")))
end
