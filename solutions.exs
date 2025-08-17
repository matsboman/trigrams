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

  **Multiple files combined:**
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

  @spec run([String.t()]) :: :ok | :noop
  def run(paths) when is_list(paths) and paths != [] do
    # Capture start time
    start_time = DateTime.utc_now()
    IO.puts("Start time: #{DateTime.to_string(start_time)}")

    Enum.each(paths, fn path ->
      IO.puts("Processing: #{path}\n")

      path
      |> token_stream()
      |> count_ngrams(@n)
      |> top_k(@top_k)
      |> Enum.each(fn {phrase, count} ->
        IO.puts("#{phrase} - #{count}")
      end)

      # spacer between files
      IO.puts("")

      # Capture end time and calculate duration
      end_time = DateTime.utc_now()
      duration = DateTime.diff(end_time, start_time, :second)

      duration_str =
        if duration >= 60, do: "#{div(duration, 60)}m #{rem(duration, 60)}s", else: "#{duration}s"

      IO.puts("End time: #{DateTime.to_string(end_time)}")
      IO.puts("Duration: #{duration_str}")
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
    # Plural possessive: dogs' -> dogs
    |> then(fn w -> Regex.replace(~r/([a-z0-9])'$/u, w, "\\1") end)
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
        window = (carry ++ [token]) |> take_last(n)

        counts =
          if length(window) == n do
            key = Enum.join(window, " ")
            Map.update(counts, key, 1, &(&1 + 1))
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
        NGram.run(paths)
    end

  true ->
    NGram.run(Enum.reject(args, &(&1 == "--test")))
end
