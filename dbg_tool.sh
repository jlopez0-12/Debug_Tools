#!/bin/bash

BASIC_PATH="/sys/module/nvme/parameters" #Here is where the debug parameters are locateds
op_code=0 #This will hold the final op code value to write to the parameter file
low_or_high="" #This flag indiactes whether high part (64-127) will set to 1 by 0x01 or 0 by 0x00

help() {    # Display usage and help information
  cat <<EOF

    =======================================================================================================
        The debug admin and I/O opcodes are defined by the NVMe specification that the drive implements
    =======================================================================================================
    
    Enable or Disable all:
      $0 -on_all      Enable all debug io and debug admin commands
      $0 -off_all     Disable all debug io and debug admin commands

      Selective cases:
        $0 -admin on_all    |   $0 -io on_all       Enable all debug io or debug admin commands
        $0 -admin off_all   |   $0 -iff on_all      Disable all debug io or debug admin commands
  
   SET DEFAULT: 
        $0 --default
  
   Usage:
      $0 -admn <op_code/s> | $0 -io <op_code/s>

        Examples of usage: 
          $0 -admn 00,02,3E
          $0 -io 00,02,3Ee
          $0 -admn 3E
          $0 -io 11

        -admn           Debug NVMe admin commands
        -io             Debug NVMe I/O commands
        <op_code/s>     Comma-separated op code/s, e.g. 00 or 02,3E or 00,02,3E

EOF
}

#Convert a hex position (0-7F) to a hex value with the corresponding bit set
convert_to_hex() { 
  local hexpos="$1"
  [[ "$hexpos" =~ ^[0-9a-fA-F]+$ ]] || { echo "Invalid hex: $hexpos" >&2; return 2; }
  local pos=$((16#$hexpos))
  printf "0x%X" $((1 << pos)) 
  #`f
  }

# Convert the provided op code positions to a single hex value with the corresponding bits set, and determine if any position is above 63 to set the low_or_high flag
position_to_hex() { 
  local cmd hex_value
  for cmd in "${op_code_array[@]}"; do
    cmd="$(xargs <<<"$cmd")"
    if (( 16#$cmd > 63 )); then
      low_or_high="0x1"
    else
      low_or_high="0x0"
      hex_value="$(convert_to_hex "$cmd")" || exit 2
      op_code=$(( op_code | hex_value ))
    fi
  done
}

#Write the final op code value to the specified parameter file
write_param() { 
  local folder="$1"
  cd "$BASIC_PATH" || { echo "Cannot cd to $BASIC_PATH" >&2; exit 2; }
  echo "Old value: $(cat "$folder")"
  op_code_hex=$(printf "0x%X,0x%X" "$op_code" "$low_or_high")
  echo "$op_code_hex" > "$folder"
  echo "Wrote $op_code_hex to $folder"
  echo "New value: $(cat "$folder")"
}

#Reset the debug parameters to their default values
default_paramters() { 
  cd "$BASIC_PATH" || { echo "Cannot cd to $BASIC_PATH" >&2; exit 2; }
  echo ""
  echo "Resetting debug parameters to default values..."
  echo ""
  echo "0xFFFFFFFFFFFFFFFF,0x1" > debug_admin_cmds
  echo "0x00,0x00" > debug_io_cmds
}

# ---- main ----
if [[ $# -eq 0 ]]; then
  echo ""
  echo "Error: Missing required arguments" >&2
  echo ""
  echo "  For usage and help: dbg_tool --help || dbg_tool -h" >&2
  echo ""
  exit 1
fi

case "$1" in
  # Display usage and help information
  "--help"|-h) 
    help
    exit 0
    ;;
  # Reset the debug parameters to their default values
  "--default") 
    default_paramters
    exit 0
  ;;
  # Enable debug for all admin and io commands
  "-on_all") 
    cd "$BASIC_PATH" || { echo "Cannot cd to $BASIC_PATH" >&2; exit 2; }
    echo "Enabling debug for all admin and io commands..."
    echo "0xFFFFFFFFFFFFFFFF,0x1" > debug_admin_cmds
    echo "0xFFFFFFFFFFFFFFFF,0x1" > debug_io_cmds
    echo "Debug enabled for all admin and io commands."
    exit 0
  ;;
  # Disable debug for all admin and io commands
  "-off_all")
    cd "$BASIC_PATH" || { echo "Cannot cd to $BASIC_PATH" >&2; exit 2; }
    echo "Disabling debug for all admin and io commands..."
    echo "0x00,0x00" > debug_admin_cmds
    echo "0x00,0x00" > debug_io_cmds
    echo "Debug disabled for all admin and io commands."
    exit 0
  ;;
  # Check for valid options and required arguments
  "-io"|"-admn")
    if [[ $# -lt 2 || -z "${2:-}" || "$2" == -* ]]; then
      echo ""
      echo "Error: ' dbg_tool $1' requires <op_code/s> (e.g. 00 or 00,02,3E)" >&2
      echo "  For usage and help: dbg_tool --help || dbg_tool -h" >&2
      echo ""
      exit 1
    fi
    # Check if it'll be enable or disable all admin or io commands
    if [[ "$2" == "on_all" && "$1" == "-io" ]]; then
      cd "$BASIC_PATH" || { echo "Cannot cd to $BASIC_PATH" >&2; exit 2; }
      echo "Enabling debug for all io commands..."
      echo "0xFFFFFFFFFFFFFFFF,0x1" > debug_io_cmds
      echo "Debug enabled for all io commands."
      exit 0
    elif [[ "$2" == "on_all" && "$1" == "-admn" ]]; then
      cd "$BASIC_PATH" || { echo "Cannot cd to $BASIC_PATH" >&2; exit 2; }
      echo "Enabling debug for all admin commands..."
      echo "0xFFFFFFFFFFFFFFFF,0x1" > debug_admin_cmds
      echo "Debug enabled for all admin commands."
      exit 0
    elif [[ "$2" == "off_all" && "$1" == "-io" ]]; then
      cd "$BASIC_PATH" || { echo "Cannot cd to $BASIC_PATH" >&2; exit 2; }
      echo "Disabling debug for all io commands..."
      echo "0x00,0x00" > debug_io_cmds
      echo "Debug disabled for all io commands."
      exit 0
    elif [[ "$2" == "off_all" && "$1" == "-admn" ]]; then
      cd "$BASIC_PATH" || { echo "Cannot cd to $BASIC_PATH" >&2; exit 2; }
      echo "Disabling debug for all admin commands..."
      echo "0x00,0x00" > debug_admin_cmds
      echo "Debug disabled for all admin commands."
      exit 0
    fi
  ;;
  *)
    echo ""
    echo "Error: Invalid option '$1'" >&2
    echo ""
    echo "  For usage and help: dbg_tool --help || dbg_tool -h" >&2
    echo ""
    exit 1
    ;;
esac

option="$1"
op_codes="$2"
IFS=',' read -ra op_code_array <<< "$op_codes"

position_to_hex

case "$option" in
  -admn)
    write_param "debug_admin_cmds" 
    ;;
  -io)
    write_param "debug_io_cmds"
    ;;
  *)
    exit 1
    ;;
esac
