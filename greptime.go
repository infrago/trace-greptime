package trace_greptime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
	"github.com/infrago/trace"
)

type (
	greptimeDriver struct{}

	greptimeConnection struct {
		instance *trace.Instance
		client   *greptime.Client
		setting  greptimeSetting
	}

	greptimeSetting struct {
		Host     string
		Port     int
		Username string
		Password string
		Database string
		Table    string
		Timeout  time.Duration
		Insecure bool
	}
)

func init() {
	infra.Register("greptime", &greptimeDriver{})
}

func (d *greptimeDriver) Connect(inst *trace.Instance) (trace.Connection, error) {
	setting := greptimeSetting{
		Host:     "127.0.0.1",
		Port:     4001,
		Database: "public",
		Table:    "traces",
		Timeout:  5 * time.Second,
		Insecure: true,
	}
	if inst != nil {
		if v, ok := getString(inst.Setting, "url"); ok && v != "" {
			applyGreptimeURL(&setting, v)
		}
		if v, ok := getString(inst.Setting, "dsn"); ok && v != "" {
			applyGreptimeURL(&setting, v)
		}
		if v, ok := getString(inst.Setting, "host"); ok && v != "" {
			setting.Host = v
		}
		if v, ok := getString(inst.Setting, "server"); ok && v != "" {
			setting.Host = v
		}
		if v, ok := getInt(inst.Setting, "port"); ok && v > 0 {
			setting.Port = v
		}
		if v, ok := getString(inst.Setting, "username"); ok {
			setting.Username = v
		}
		if v, ok := getString(inst.Setting, "user"); ok && setting.Username == "" {
			setting.Username = v
		}
		if v, ok := getString(inst.Setting, "password"); ok {
			setting.Password = v
		}
		if v, ok := getString(inst.Setting, "pass"); ok && setting.Password == "" {
			setting.Password = v
		}
		if v, ok := getString(inst.Setting, "database"); ok && v != "" {
			setting.Database = v
		}
		if v, ok := getString(inst.Setting, "db"); ok && v != "" {
			setting.Database = v
		}
		if v, ok := getString(inst.Setting, "table"); ok && v != "" {
			setting.Table = v
		}
		if v, ok := getDuration(inst.Setting, "timeout"); ok && v > 0 {
			setting.Timeout = v
		}
		if v, ok := getBool(inst.Setting, "insecure"); ok {
			setting.Insecure = v
		}
		if v, ok := getBool(inst.Setting, "tls"); ok {
			setting.Insecure = !v
		}
	}
	return &greptimeConnection{instance: inst, setting: setting}, nil
}

func applyGreptimeURL(setting *greptimeSetting, raw string) {
	if setting == nil {
		return
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return
	}
	if !strings.Contains(raw, "://") {
		raw = "greptime://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u == nil {
		return
	}
	if host := strings.TrimSpace(u.Hostname()); host != "" {
		setting.Host = host
	}
	if p := strings.TrimSpace(u.Port()); p != "" {
		if port, err := strconv.Atoi(p); err == nil && port > 0 {
			setting.Port = port
		}
	}
	if u.User != nil {
		if user := strings.TrimSpace(u.User.Username()); user != "" {
			setting.Username = user
		}
		if pass, ok := u.User.Password(); ok {
			setting.Password = pass
		}
	}
	path := strings.TrimSpace(strings.TrimPrefix(u.Path, "/"))
	if path != "" {
		parts := strings.Split(path, "/")
		if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
			setting.Database = strings.TrimSpace(parts[0])
		}
		if len(parts) > 1 && strings.TrimSpace(parts[1]) != "" {
			setting.Table = strings.TrimSpace(parts[1])
		}
	}
	q := u.Query()
	if v := strings.TrimSpace(q.Get("database")); v != "" {
		setting.Database = v
	}
	if v := strings.TrimSpace(q.Get("db")); v != "" {
		setting.Database = v
	}
	if v := strings.TrimSpace(q.Get("table")); v != "" {
		setting.Table = v
	}
	if v := strings.TrimSpace(q.Get("username")); v != "" {
		setting.Username = v
	}
	if v := strings.TrimSpace(q.Get("user")); v != "" {
		setting.Username = v
	}
	if v := strings.TrimSpace(q.Get("password")); v != "" {
		setting.Password = v
	}
	if v := strings.TrimSpace(q.Get("pass")); v != "" {
		setting.Password = v
	}
	if v := strings.TrimSpace(q.Get("timeout")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			setting.Timeout = d
		}
	}
	if v := strings.TrimSpace(q.Get("insecure")); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			setting.Insecure = b
		}
	}
	if v := strings.TrimSpace(q.Get("tls")); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			setting.Insecure = !b
		}
	}
}

func (c *greptimeConnection) Open() error {
	cfg := greptime.NewConfig(c.setting.Host).
		WithPort(c.setting.Port).
		WithDatabase(c.setting.Database).
		WithAuth(c.setting.Username, c.setting.Password).
		WithInsecure(c.setting.Insecure)
	client, err := greptime.NewClient(cfg)
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

func (c *greptimeConnection) Close() error { return nil }

func (c *greptimeConnection) Write(spans ...trace.Span) error {
	if c.client == nil || len(spans) == 0 {
		return nil
	}
	tbl, err := table.New(c.setting.Table)
	if err != nil {
		return err
	}
	_ = tbl.WithSanitate(false)

	fieldMap := trace.ResolveFields(c.instance.Config.Fields, greptimeDefaultFields())
	pairs := orderedPairs(fieldMap)
	for _, p := range pairs {
		kind, dtype := greptimeFieldSpec(p.source)
		switch kind {
		case "tag":
			_ = tbl.AddTagColumn(p.target, dtype)
		case "timestamp":
			_ = tbl.AddTimestampColumn(p.target, dtype)
		default:
			_ = tbl.AddFieldColumn(p.target, dtype)
		}
	}

	for _, span := range spans {
		values := trace.SpanValues(span, c.instance.Name, c.instance.Config.Flag)
		row := make([]any, 0, len(pairs))
		for _, p := range pairs {
			row = append(row, convertGreptimeValue(p.source, values))
		}
		if err := tbl.AddRow(row...); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()
	_, err = c.client.Write(ctx, tbl)
	return err
}

type fieldPair struct {
	source string
	target string
}

func greptimeDefaultFields() map[string]string {
	return map[string]string{
		"project":        "project",
		"profile":        "profile",
		"node":           "node",
		"time":           "timestamp",
		"start":          "start_time_unix_nano",
		"end":            "end_time_unix_nano",
		"duration":       "duration_nano",
		"duration_nano":  "duration_nano",
		"service_name":   "service_name",
		"span_name":      "span_name",
		"step":           "step",
		"trace_id":       "trace_id",
		"span_id":        "span_id",
		"parent_id":      "parent_id",
		"parent_span_id": "parent_span_id",
		"kind":           "kind",
		"entry":          "entry",
		"status":         "status",
		"code":           "code",
		"result":         "result",
		"attributes":     "attributes",
		"resource":       "resource",
	}
}

func orderedPairs(fields map[string]string) []fieldPair {
	order := []string{
		"project", "profile", "node", "time", "start", "end", "duration", "duration_nano",
		"service_name", "span_name", "step",
		"trace_id", "span_id", "parent_id", "parent_span_id",
		"kind", "entry", "status", "code", "result",
		"attributes", "resource",
	}
	pairs := make([]fieldPair, 0, len(fields))
	used := map[string]bool{}
	usedTarget := map[string]bool{}
	for _, source := range order {
		if target, ok := fields[source]; ok && target != "" {
			target = strings.TrimSpace(target)
			if target == "" || usedTarget[target] {
				used[source] = true
				continue
			}
			pairs = append(pairs, fieldPair{source: source, target: target})
			used[source] = true
			usedTarget[target] = true
		}
	}
	extras := make([]string, 0)
	for source, target := range fields {
		if target == "" || used[source] {
			continue
		}
		extras = append(extras, source)
	}
	sort.Strings(extras)
	for _, source := range extras {
		target := strings.TrimSpace(fields[source])
		if target == "" || usedTarget[target] {
			continue
		}
		pairs = append(pairs, fieldPair{source: source, target: target})
		usedTarget[target] = true
	}
	return pairs
}

func greptimeFieldSpec(source string) (string, types.ColumnType) {
	switch source {
	case "project", "profile", "node", "service_name", "span_name", "step", "entry":
		return "tag", types.STRING
	case "time":
		return "timestamp", types.TIMESTAMP_NANOSECOND
	case "start", "end", "duration", "duration_nano", "code":
		return "field", types.INT64
	default:
		return "field", types.STRING
	}
}

func convertGreptimeValue(source string, values map[string]any) any {
	v, ok := values[source]
	if !ok {
		return nil
	}
	switch source {
	case "time":
		switch vv := v.(type) {
		case time.Time:
			if vv.IsZero() {
				return nil
			}
			return vv
		case int64:
			if vv == 0 {
				return nil
			}
			return time.Unix(0, vv)
		case int:
			if vv == 0 {
				return nil
			}
			return time.Unix(0, int64(vv))
		case float64:
			if vv == 0 {
				return nil
			}
			return time.Unix(0, int64(vv))
		case string:
			s := strings.TrimSpace(vv)
			if s == "" {
				return nil
			}
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				if n == 0 {
					return nil
				}
				return time.Unix(0, n)
			}
			// Keep parsing permissive for custom string formats.
			if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
				if t.IsZero() {
					return nil
				}
				return t
			}
			if t, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", s); err == nil {
				if t.IsZero() {
					return nil
				}
				return t
			}
		}
		return nil
	case "start", "end", "duration", "duration_nano", "code":
		switch vv := v.(type) {
		case int64:
			if vv == 0 {
				return nil
			}
			return vv
		case int:
			if vv == 0 {
				return nil
			}
			return int64(vv)
		case float64:
			if vv == 0 {
				return nil
			}
			return int64(vv)
		case string:
			s := strings.TrimSpace(vv)
			if s == "" {
				return nil
			}
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				if n == 0 {
					return nil
				}
				return n
			}
		}
		return nil
	case "attributes", "resource":
		switch vv := v.(type) {
		case Map:
			if len(vv) == 0 {
				return nil
			}
			if b, err := json.Marshal(vv); err == nil {
				return string(b)
			}
		}
		if isEmptyGreptimeValue(v) {
			return nil
		}
		return fmt.Sprintf("%v", v)
	default:
		if isEmptyGreptimeValue(v) {
			return nil
		}
		return fmt.Sprintf("%v", v)
	}
}

func isEmptyGreptimeValue(v any) bool {
	switch vv := v.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(vv) == ""
	case []byte:
		return len(vv) == 0
	case []any:
		return len(vv) == 0
	case []string:
		return len(vv) == 0
	case map[string]any:
		return len(vv) == 0
	default:
		return false
	}
}

func getString(m Map, key string) (string, bool) {
	if m == nil {
		return "", false
	}
	val, ok := m[key]
	if !ok {
		return "", false
	}
	v, ok := val.(string)
	return v, ok
}

func getInt(m Map, key string) (int, bool) {
	if m == nil {
		return 0, false
	}
	val, ok := m[key]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

func getDuration(m Map, key string) (time.Duration, bool) {
	if m == nil {
		return 0, false
	}
	val, ok := m[key]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case time.Duration:
		return v, true
	case int:
		return time.Second * time.Duration(v), true
	case int64:
		return time.Second * time.Duration(v), true
	case float64:
		return time.Second * time.Duration(v), true
	case string:
		d, err := time.ParseDuration(v)
		if err == nil {
			return d, true
		}
	}
	return 0, false
}

func getBool(m Map, key string) (bool, bool) {
	if m == nil {
		return false, false
	}
	val, ok := m[key]
	if !ok {
		return false, false
	}
	v, ok := val.(bool)
	return v, ok
}
