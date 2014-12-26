function get_nullable{K, V}(dict :: Dict{K, V}, key :: K)
  if haskey(dict, key)
    Nullable{V}(dict[key])
  else
    Nullable{V}()
  end
end
